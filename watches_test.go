package main

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

var subject = "watchManager"

type testNotificationStore struct {
	notifications []Notification
}

func (s *testNotificationStore) append(topic string, v interface{}) error {
	s.notifications = append(s.notifications, Notification{
		Index:     uint64(len(s.notifications) + 1),
		Timestamp: time.Now(),
		Data:      v,
	})
	return nil
}

func (s *testNotificationStore) get(topic string, generationID string, fromIndex uint64) (*NotificationsResponse, error) {
	i := int(fromIndex)
	return &NotificationsResponse{
		GenerationID:  generationID,
		Notifications: s.notifications[i:],
	}, nil
}

func (s *testNotificationStore) close() error {
	return nil
}

func TestWatch(t *testing.T) {
	var tests = []struct {
		context      string
		expectation  string
		messageCount int
		messageDelay time.Duration
		pushInterval time.Duration
	}{
		{
			context:      "New messages created every 1s",
			expectation:  "send messages to client every pushInterval",
			messageCount: 10,
			messageDelay: time.Second,
			pushInterval: time.Second * 2,
		},
	}

	for _, test := range tests {
		runWatchTest(t, test)
	}
}

func runWatchTest(t *testing.T, test struct {
	context      string
	expectation  string
	messageCount int
	messageDelay time.Duration
	pushInterval time.Duration
}) {
	t.Logf("When %s, %s should %s", test.context, subject, test.expectation)

	store := &testNotificationStore{}
	dialer := websocket.DefaultDialer
	r := mux.NewRouter()
	watchManager := newWatchManager(store, test.pushInterval)

	r.HandleFunc("/{topic}/watch", watchManager.handleWatchRequest)
	server := httptest.NewServer(r)
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Scheme = "ws"
	u.Path = "/mytopic/watch"

	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("unexpected error connecting: %v\nresponse: %#v", err, resp)
	}
	messageChan := make(chan *NotificationsResponse)
	go func() {
		defer close(messageChan)
		for {
			var notificationsResponse *NotificationsResponse
			if err := conn.ReadJSON(&notificationsResponse); err != nil {
				return
			}
			messageChan <- notificationsResponse
		}
	}()

	submittedMessages := []string{}
	go func() {
		for i := 0; i < test.messageCount; i++ {
			item := fmt.Sprintf("{test packet #%v}", i)
			store.append("testtopic", item)
			submittedMessages = append(submittedMessages, item)
			time.Sleep(test.messageDelay)
		}
	}()

	receivedItems := 0
	for {
		select {
		case <-time.After((test.pushInterval + test.messageDelay) * time.Duration(test.messageCount)):
			t.Fatal("timed out waiting for messages to be received")
		case notificationsResponse := <-messageChan:
			for _, notification := range notificationsResponse.Notifications {
				item := notification.Data.(string)
				if item != submittedMessages[receivedItems] {
					t.Fatalf("expected received message %s to equal sent message %s", item, submittedMessages[receivedItems])
				}
				receivedItems++
				if receivedItems == test.messageCount {
					return
				}
			}
		}
	}

}
