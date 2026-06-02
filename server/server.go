package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Configuration struct {
	LobbyExpiration     time.Duration
	HeartbeatInterval   time.Duration
	LobbyMaxCount       int
	LobbyMaxClientCount int
	MessageSizeLimit    int // defines the max message size in bytes

	FilterClient  func(*http.Request) bool
	FilterMessage func(*http.Request, []byte) bool
}

var DefaultConfiguration = Configuration{
	LobbyExpiration:     5 * time.Minute,
	HeartbeatInterval:   2 * time.Minute,
	LobbyMaxCount:       1000,
	LobbyMaxClientCount: 10,
	MessageSizeLimit:    100,
}

type Message struct {
	IsSystem bool            `json:"is_system"`
	Sender   string          `json:"sender,omitempty"` // optional string label indicating who was the source of the message
	Content  json.RawMessage `json:"body"`
}

type Client struct {
	CurrentToken string
	CaptionName  string
	Feed         chan Message

	authLock sync.Mutex
}

const (
	LobbyStatusOpen int = iota
	LobbyStatusClosed
)

type Lobby struct {
	Clients   sync.Map
	ExpiresAt time.Time

	ccmu        sync.Mutex
	clientCount int

	hmu     sync.Mutex
	History []Message

	smu    sync.Mutex
	Status int
}

func (r *Lobby) GetClient(uuid string) *Client {
	if c, ok := r.Clients.Load(uuid); ok {
		return c.(*Client)
	}
	return nil
}

func (r *Lobby) Broadcast(msg Message) {
	r.hmu.Lock()
	r.History = append(r.History, msg)
	r.hmu.Unlock()
	r.Clients.Range(func(_, value any) bool {
		c := value.(*Client)
		go func() {
			c.Feed <- msg
		}()
		return true
	})
}

func (r *Lobby) Close() {
	r.Clients.Range(func(_, value any) bool {
		c := value.(*Client)
		go func() {
			c.Feed <- Message{
				IsSystem: true,
			} // empty system message implies closing of lobby
		}()
		return true
	})
}

type LobbyStore struct {
	c Configuration
	s sync.Map

	lcmu       sync.Mutex
	lobbyCount int
}

func (ls *LobbyStore) GetOrCreateLobby(idx string) *Lobby {
	r, ok := ls.s.Load(idx)
	ls.lcmu.Lock()
	// if lobby not found && lobby maximum is not reached
	if !ok && ls.lobbyCount < ls.c.LobbyMaxCount {
		r = &Lobby{
			ExpiresAt: time.Now().Add(ls.c.LobbyExpiration),
			Status:    LobbyStatusOpen,
		}
		ls.s.Store(idx, r)
		ls.lobbyCount++
	}
	ls.lcmu.Unlock()

	if r == nil {
		return nil
	}

	return r.(*Lobby)
}

func (ls *LobbyStore) EnterLobby(lobby *Lobby, clientIdx, captionName string) *Client {
	_, ok := lobby.Clients.Load(clientIdx)
	// if client is already in the lobby; or the max number of clients is reached
	if ok {
		return nil
	}

	display := captionName
	if display == "" {
		display = clientIdx
	}

	var c *Client
	lobby.ccmu.Lock()
	if lobby.clientCount < ls.c.LobbyMaxClientCount && lobby.Status == LobbyStatusOpen {
		feed := make(chan Message) // allow one buffered message, so the initial auth message can be added to the feed
		c = &Client{
			CaptionName: display,
			Feed:        feed,
		}
		lobby.Clients.Store(clientIdx, c)
		lobby.clientCount++
		ls.ResetClientToken(c) // this uses the hard quote of 1 for stored messages on the channel
		go func() {
			em, err := json.Marshal(ExpirationMessage{
				ExpiresAt: lobby.ExpiresAt,
			})
			if err == nil { // as this is a usability feature, we can just skip the message
				go func() {
					c.Feed <- Message{
						IsSystem: true,
						Content:  em,
					}
				}()
			} else {
				slog.Error("expiration message could not be created", "err", err)
			}

			lobby.hmu.Lock()
			for _, m := range lobby.History {
				go func() {
					c.Feed <- m
				}()
			}
			lobby.hmu.Unlock()
		}()
	}
	lobby.ccmu.Unlock()

	return c
}

func (ls *LobbyStore) ExitLobby(lobby *Lobby, clientIdx string) {
	_, ok := lobby.Clients.Load(clientIdx)
	if ok {
		lobby.ccmu.Lock()
		lobby.Clients.Delete(clientIdx)
		lobby.clientCount--
		if lobby.clientCount < 1 {
			lobby.smu.Lock()
			lobby.Status = LobbyStatusClosed
			lobby.smu.Unlock()
		}
		lobby.ccmu.Unlock()
	}
}

func (ls *LobbyStore) Cleanup(threshold time.Time) {
	var toDelete []string
	ls.s.Range(func(key, value any) bool {
		r := value.(*Lobby)
		if r.ExpiresAt.Before(threshold) || r.Status == LobbyStatusClosed {
			r.Close()
			toDelete = append(toDelete, key.(string))
		}
		return true
	})

	for _, v := range toDelete {
		ls.s.Delete(v)
		ls.lcmu.Lock()
		ls.lobbyCount--
		ls.lcmu.Unlock()
	}

}

func (ls *LobbyStore) ResetClientToken(client *Client) {
	client.authLock.Lock()
	defer client.authLock.Unlock()

	token := uuid.NewString()
	client.CurrentToken = token
	msg, err := json.Marshal(AuthMessage{
		Token: token,
	})
	if err != nil {
		slog.Error("failed to generate auth message for client", "err", err)
		return
	}

	go func() {
		client.Feed <- Message{
			IsSystem: true,
			Content:  msg,
		}
	}()
}

type LobbyMux struct {
	mux   *http.ServeMux
	store *LobbyStore
}

func NewLobbyMux(config ...Configuration) *LobbyMux {
	c := DefaultConfiguration
	if len(config) > 0 {
		c = config[0]
	}

	// guard against a zero/unset interval; time.NewTicker panics on <= 0
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultConfiguration.HeartbeatInterval
	}

	s := &LobbyStore{c: c}
	go func() {
		for {
			<-time.After(time.Second)
			s.Cleanup(time.Now())
		}
	}()

	m := http.NewServeMux()
	m.HandleFunc("/connect/{lobbyIdx}/{clientIdx}", func(w http.ResponseWriter, r *http.Request) {
		// on connect and after each message send, the client receives the authentication
		// token for the next send from the client
		// when the lobby didn't exist; the lobby will be created on connect
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		lobby := s.GetOrCreateLobby(r.PathValue("lobbyIdx"))
		if lobby == nil {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		slog.Info("lobby available")

		allowed := true
		if c.FilterClient != nil {
			allowed = c.FilterClient(r)
		}

		if !allowed {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		client := s.EnterLobby(lobby, r.PathValue("clientIdx"), r.URL.Query().Get("captionName"))
		if client == nil {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		slog.Info("client connected to lobby")

		w.Header().Set("cache-control", "no-store")
		w.Header().Set("content-type", "text/event-stream")

		disconnected := make(chan bool)
		go func() {
			<-r.Context().Done()
			slog.Info("client disconnected")
			s.ExitLobby(lobby, r.PathValue("clientIdx"))
			disconnected <- true
		}()

		iddleTicker := time.NewTicker(c.HeartbeatInterval)

		for {
			select {
			case <-disconnected:
				iddleTicker.Stop()
				return

			case msg := <-client.Feed:
				if msg.IsSystem && len(msg.Content) == 0 {
					slog.Info("client left lobby")

					s.ExitLobby(lobby, r.PathValue("clientIdx"))
					return // exit if empty system message
				}

				mm, err := json.Marshal(msg)
				sb := &strings.Builder{}
				sb.Write(mm)
				if err != nil {
					slog.Error("failed marshalling message", "err", err)
				}
				_, err = w.Write(
					[]byte(fmt.Sprintf(
						"data: %s\n\n",
						sb.String(),
					)),
				)
				if err != nil {
					slog.Error("failed to write message", "err", err)
				}

				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			case <-iddleTicker.C:
				_, err := w.Write([]byte(": ping\n\n"))
				if err != nil {
					slog.Error("failed to write message", "err", err)
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	})

	tokenRe := regexp.MustCompile(`Token\s+(.+)`)
	m.HandleFunc("/send/{lobbyIdx}/{clientIdx}", func(w http.ResponseWriter, r *http.Request) {
		// on send the client must provide the authentication token that it received for its next reply;
		// if either the lobby doesn't exists, the client doesn't exists or the authentication token
		// assigned to the client isn't correct the send is blocked
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		lobby := s.GetOrCreateLobby(r.PathValue("lobbyIdx"))
		if lobby == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		client := lobby.GetClient(r.PathValue("clientIdx"))
		if client == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		tokens := tokenRe.FindStringSubmatch(r.Header.Get("Authorization"))
		if len(tokens) < 2 || client.CurrentToken != tokens[1] {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		slog.Info("message received from client on lobby")

		r.Body = http.MaxBytesReader(w, r.Body, int64(c.MessageSizeLimit))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError) // the error type here is unfortunate, but more simplistic
			return
		}

		allowed := true
		if c.FilterMessage != nil {
			allowed = c.FilterMessage(r, body)
		}

		if !allowed {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		lobby.Broadcast(Message{
			IsSystem: false,
			Sender:   client.CaptionName,
			Content:  body,
		})

		s.ResetClientToken(client)
	})
	return &LobbyMux{
		mux:   m,
		store: s,
	}
}

func (l *LobbyMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mux.ServeHTTP(w, r)
}
