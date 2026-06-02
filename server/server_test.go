package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"
)

func sseClient(srv *httptest.Server, lobbyIdx, clientIdx string, onMessage func(msg []byte, reply func(string, string) int) bool) (bool, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan bool)
	go func() {
		r, err := http.NewRequestWithContext(ctx, "GET", srv.URL+fmt.Sprintf("/connect/%s/%s", lobbyIdx, clientIdx), nil)
		if err != nil {
			done <- false
			return
		}
		c, err := srv.Client().Do(r)
		if c != nil {
			slog.Info("sse client connection response", "status", c.StatusCode)
		}
		if err != nil || c.StatusCode >= http.StatusBadRequest {
			done <- false
			return
		}

		body := bufio.NewReader(c.Body)
		for {
			l, _, err := body.ReadLine()
			if err != nil {
				done <- false
				return
			}

			if len(l) > 0 {
				if onMessage(l, func(token, msg string) int {
					msgr, err := http.NewRequestWithContext(ctx, "POST", srv.URL+fmt.Sprintf("/send/%s/%s", lobbyIdx, clientIdx), bytes.NewBufferString(msg))
					if err != nil {
						done <- false
						return -1
					}
					msgr.Header.Set("Authorization", "Token "+token)

					msgres, err := srv.Client().Do(msgr)
					if err != nil {
						done <- false
						return -1
					}

					if msgres != nil {
						slog.Info("sse client responding", "status", msgres.StatusCode)
					}
					if err != nil || msgres.StatusCode >= http.StatusBadRequest {
						done <- false
						return msgres.StatusCode
					}
					if msgres != nil {
						return msgres.StatusCode
					} else {
						return -1
					}
				}) == true {
					done <- true
					return
				}
			}
		}
	}()
	msg := <-done
	return msg, func() {
		cancel()
	}
}

func TestLobbyMux(t *testing.T) {
	t.Run("client can connect and receive data; and disconnects on lobby close", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		if result == false {
			t.Error("didn't connect or receive data")
		}
	})

	t.Run("validate a lobby is cleaned up after the last one leaves", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 10,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		ctx, cancel := context.WithCancel(context.Background())
		r, err := http.NewRequestWithContext(ctx, "GET", s.URL+fmt.Sprintf("/connect/%s/%s", "lobby-1", "client-1"), nil)
		if err != nil {
			t.Error(err)
		}

		c, err := s.Client().Do(r)
		if c != nil {
			slog.Info("sse client connection response", "status", c.StatusCode)
		}

		cancel()
		time.Sleep(2 * time.Second)

		if lm.store.lobbyCount > 0 {
			t.Error("should've cleaned up the lobby")
		}
	})

	t.Run("validate a lobby is cleaned up after its expiration", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		r, err := http.NewRequestWithContext(context.Background(), "GET", s.URL+fmt.Sprintf("/connect/%s/%s", "lobby-1", "client-1"), nil)
		if err != nil {
			t.Error(err)
		}

		c, err := s.Client().Do(r)
		if c != nil {
			slog.Info("sse client connection response", "status", c.StatusCode)
		}

		time.Sleep(4 * time.Second)

		if lm.store.lobbyCount > 0 {
			t.Error("should've cleaned up the lobby")
		}
	})

	t.Run("client can connect and send & receive sent data", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, reply func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				re := regexp.MustCompile(`data:\s(.+)`)
				e := map[string]any{}
				m := re.FindSubmatch(msg)
				_ = json.Unmarshal(m[1], &e)
				body := e["body"].(map[string]any)
				token := body["token"]
				reply(token.(string), `{"msg": "hello world"}`)
				return false
			}

			if bytes.Contains(msg, []byte("hello world")) {
				slog.Info("received sent message")
				return true
			}

			return false
		})

		if result == false {
			t.Error("didn't connect or receive data")
		}
	})

	t.Run("lobby limit can't be exceeded", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       0,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		if result == true {
			t.Error("didn't abide limit")
		}
	})

	t.Run("client per lobby limit can't be exceeded", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 0,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		if result == true {
			t.Error("didn't abide limit")
		}
	})

	t.Run("client only can connect once to a lobby", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       2,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		one, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		two, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		if one == two {
			t.Error("expected one client to connect, the other not; but both got same result")
		}
	})

	t.Run("authentication token is only good for one message", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       2,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				re := regexp.MustCompile(`data:\s(.+)`)
				e := map[string]any{}
				m := re.FindSubmatch(msg)
				_ = json.Unmarshal(m[1], &e)
				body := e["body"].(map[string]any)
				token := body["token"]

				msgr, err := http.NewRequestWithContext(ctx, "POST", s.URL+fmt.Sprintf("/send/%s/%s", "lobby-1", "client-1"), bytes.NewBufferString(""))
				if err != nil {
					t.Error(err)
					return false
				}
				msgr.Header.Set("Authorization", "Token "+token.(string))
				first, err := s.Client().Do(msgr)
				if err != nil {
					t.Error(err)
					return false
				}

				second, err := s.Client().Do(msgr)
				if err != nil {
					t.Error(err)
					return false
				}

				if first.StatusCode == http.StatusOK && second.StatusCode == http.StatusUnauthorized {
					return true
				}
			}
			return false
		})

		if result != true {
			t.Error("failed to reach expected state")
		}
	})

	t.Run("when a client connects, it receives the full history of what was sent before", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		lobby := lm.store.GetOrCreateLobby("lobby-1")
		lobby.History = append(lobby.History, Message{
			IsSystem: false,
			Content:  []byte(`{"msg": "hello"}`),
		})
		lobby.History = append(lobby.History, Message{
			IsSystem: false,
			Content:  []byte(`{"msg": "world"}`),
		})

		receivedHello := false
		receivedWorld := false
		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("hello")) {
				receivedHello = true
			}
			if bytes.Contains(msg, []byte("world")) {
				receivedWorld = true
			}
			if receivedHello && receivedWorld {
				return true
			}
			return false
		})
		if result != true {
			t.Error("failed to receive history")
		}
	})

	t.Run("only once enter lobby", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 2,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		one, stop := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				return true
			}
			return false
		})
		stop()
		two, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				return true
			}
			return false
		})

		if (one == false && two == false) || (one == true && two == true) {
			t.Error("failed to raise error on double connection")
			return
		}
	})

	t.Run("a message can't exceed the message size limit", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    10,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		statusCode := -1
		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, reply func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				re := regexp.MustCompile(`data:\s(.+)`)
				e := map[string]any{}
				m := re.FindSubmatch(msg)
				_ = json.Unmarshal(m[1], &e)
				body := e["body"].(map[string]any)
				token := body["token"]
				statusCode = reply(token.(string), `{"msg": "a very very very very very very very very very long message"}`)
				return true
			}
			return false
		})

		if result != false || statusCode != http.StatusInternalServerError {
			t.Error("didn't connect or receive data")
		}
	})

	t.Run("if client filter is set, connect request are filtered out if the function returns false", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
			FilterClient: func(request *http.Request) bool {
				return false
			},
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				return true
			}
			return false
		})

		if result != false {
			t.Error("should've been filtered out")
		}
	})

	t.Run("if message filter is set, send requests are filtered out if the function returns false", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 1,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
			FilterMessage: func(request *http.Request, i []byte) bool {
				return false
			},
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		statusCode := -1
		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, reply func(string, string) int) bool {
			if bytes.Contains(msg, []byte("token")) {
				slog.Info("received token")
				re := regexp.MustCompile(`data:\s(.+)`)
				e := map[string]any{}
				m := re.FindSubmatch(msg)
				_ = json.Unmarshal(m[1], &e)
				body := e["body"].(map[string]any)
				token := body["token"]
				statusCode = reply(token.(string), `{"msg": "hello world"}`)
				return true
			}

			return false
		})

		if result != false || statusCode != http.StatusBadRequest {
			t.Error("should've filtered out message")
		}
	})

	t.Run("server sends a heartbeat within the configured interval to keep iddle clients connected", func(t *testing.T) {
		lm := NewLobbyMux(Configuration{
			LobbyExpiration:     time.Second * 10,
			HeartbeatInterval:   time.Millisecond * 200,
			LobbyMaxCount:       1,
			LobbyMaxClientCount: 1,
			MessageSizeLimit:    100,
		})
		s := httptest.NewServer(lm)
		defer s.Close()

		result, _ := sseClient(s, "lobby-1", "client-1", func(msg []byte, _ func(string, string) int) bool {
			if bytes.Contains(msg, []byte(": ping")) {
				slog.Info("heartbeat received")
				return true
			}
			return false
		})

		if result == false {
			t.Error("should've receive a heartbeat ping within the timeout")
		}
	})
}
