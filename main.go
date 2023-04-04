package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
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
	messages []int64
	mut      sync.RWMutex
}

func NewReadMessages() ReadMessages {
	return ReadMessages{
		messages: make([]int64, 0),
	}
}

func (m *ReadMessages) contains(target int64) bool {
	m.mut.RLock()
	defer m.mut.RUnlock()
	return slices.Contains(m.messages, target)
}

func (m *ReadMessages) update(message int64) {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.messages = append(m.messages, message)
}

func (m *ReadMessages) keys() []int64 {
	return m.messages
}

type SentMessages struct {
	messages map[int64][]string
	mut      sync.RWMutex
}

func (m *SentMessages) update(message int64, dest string) {
	m.mut.Lock()
	defer m.mut.Unlock()
	m.messages[message] = append(m.messages[message], dest)
}

func (m *SentMessages) hasSucceeded(message int64, dest string) bool {
	m.mut.RLock()
	defer m.mut.RUnlock()
	if m.messages[message] == nil {
		return false
	}
	return slices.Contains(m.messages[message], dest)
}

func (m *SentMessages) checkUnsent(message int64, topology []string) []string {
	m.mut.RLock()
	defer m.mut.RUnlock()
	unsent := make([]string, 0)

	if len(m.messages[message]) == len(topology) {
		return unsent
	}

	for _, node := range topology {
		if !m.hasSucceeded(message, node) {
			unsent = append(unsent, node)
		}
	}
	return unsent
}

func NewSentMessages() SentMessages {
	return SentMessages{
		messages: make(map[int64][]string),
	}
}

func createReadResponse(messages []int64) ReadResponse {
	return ReadResponse{
		Type:     "read_ok",
		Messages: messages,
	}
}

func main() {
	var wg sync.WaitGroup
	n := maelstrom.NewNode()
	readMessages := NewReadMessages()
	sentMessages := NewSentMessages()
	topology := make([]string, 0)

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

		topology = body.Topology[n.ID()]

		topology_response := make(map[string]string)
		topology_response["type"] = "topology_ok"
		return n.Reply(msg, topology_response)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		var body BroadcastRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		log.Printf("Before update %v | %v", body.Message, msg.Dest)
		sentMessages.update(body.Message, msg.Dest) // this is deadlocking
		log.Printf("After update %v | %v", body.Message, msg.Dest)

		log.Printf("body.Message=%v msg.Dest=%v msg.Body=%v msg.Src=%v", body.Message, msg.Dest, string(msg.Body), msg.Src)

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		broadcast_response := make(map[string]any)
		broadcast_response["type"] = "broadcast_ok"

		var body BroadcastRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if readMessages.contains(body.Message) {
			return n.Reply(msg, broadcast_response)
		}
		readMessages.update(body.Message)

		if slices.Contains(topology, msg.Src) {
			sentMessages.update(body.Message, msg.Src)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for len(sentMessages.checkUnsent(body.Message, topology)) > 0 {
				for _, node := range sentMessages.checkUnsent(body.Message, topology) {
					n.Send(node, body)
				}
				time.Sleep(time.Second)
			}
		}()

		if msg.Src[0] == 'n' {
			broadcast_response["message"] = body.Message
		}

		return n.Reply(msg, broadcast_response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := createReadResponse(readMessages.keys())
		return n.Reply(msg, body)
	})

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		wg.Wait()
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()
	wg2.Wait()
}
