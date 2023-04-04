package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type BroadcastRequest struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

type ReadResponse struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

type ReadMessages struct {
	messages cmap.ConcurrentMap[string, bool]
}

func (m *ReadMessages) contains(target string) bool {
	_, ok := m.messages.Get(target)
	return ok
}

func (m *ReadMessages) acknowledge(message string) {
	m.messages.Set(message, true)
}

func (m *ReadMessages) keys() []string {
	keys := make([]string, 0)
	for entry := range m.messages.IterBuffered() {
		keys = append(keys, entry.Key)
	}
	return keys
}

func createReadResponse(messages []string) ReadResponse {
	int_messages := make([]int64, 0)
	for _, message := range messages {
		if int_message, err := strconv.Atoi(message); err != nil {
			int_messages = append(int_messages, int64(int_message))
		}
	}

	return ReadResponse{
		Type:     "read_ok",
		Messages: int_messages,
	}
}

func main() {
	n := maelstrom.NewNode()
	messages_read := ReadMessages{
		messages: cmap.New[bool](),
	}
	topology := make(map[string][]string)

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology

		topology_response := make(map[string]string)
		topology_response["type"] = "topology_ok"
		return n.Reply(msg, topology_response)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		broadcast_response := make(map[string]string)
		broadcast_response["type"] = "broadcast_ok"

		var body BroadcastRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if messages_read.contains(fmt.Sprint(body.Message)) {
			return n.Reply(msg, broadcast_response)
		}

		messages_read.acknowledge(fmt.Sprint(body.Message))

		for _, dest := range topology[n.ID()] {
			dest := dest
			go func() {
				succeeded := false
				var delay int64 = 120
				for !succeeded {
					n.RPC(dest, body, func(msg maelstrom.Message) error {
						succeeded = true
						return nil
					})
					delay = int64(math.Min(float64(delay), 3000))
					time.Sleep(time.Duration(delay * 1000))
					delay = delay * 2
				}
			}()
		}

		return n.Reply(msg, broadcast_response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := createReadResponse(messages_read.keys())
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
