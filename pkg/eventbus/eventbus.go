package eventbus

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Topic represents an event topic.
type Topic string

const (
	TopicTick      Topic = "tick"
	TopicCandle    Topic = "candle"
	TopicOrder     Topic = "order"
	TopicTrade     Topic = "trade"
	TopicPosition  Topic = "position"
	TopicError     Topic = "error"
	TopicStrategy  Topic = "strategy"
	TopicOrderBook Topic = "orderbook"
)

// Event represents a published event.
type Event struct {
	Topic     Topic      `json:"topic"`
	Timestamp time.Time  `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// Handler is a callback for events.
type Handler func(event Event)

// EventBus provides pub/sub functionality.
type EventBus struct {
	mu        sync.RWMutex
	subs      map[Topic][]Handler
	eventChan chan Event
	quit      chan struct{}
	wg        sync.WaitGroup
}

// New creates a new EventBus.
func New() *EventBus {
	bus := &EventBus{
		subs:      make(map[Topic][]Handler),
		eventChan: make(chan Event, 1024),
		quit:      make(chan struct{}),
	}
	bus.wg.Add(1)
	go bus.dispatch()
	return bus
}

// Subscribe registers a handler for a topic.
func (b *EventBus) Subscribe(topic Topic, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], handler)
	log.Debug().Str("topic", string(topic)).Msg("subscribed to event bus")
}

// Publish sends an event asynchronously.
func (b *EventBus) Publish(topic Topic, payload interface{}) {
	event := Event{
		Topic:     topic,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	select {
	case b.eventChan <- event:
	default:
		log.Warn().Str("topic", string(topic)).Msg("event bus channel full, dropping event")
	}
}

// dispatch processes events from the channel.
func (b *EventBus) dispatch() {
	defer b.wg.Done()
	for {
		select {
		case event := <-b.eventChan:
			b.mu.RLock()
			handlers, ok := b.subs[event.Topic]
			b.mu.RUnlock()
			if !ok {
				continue
			}
			for _, handler := range handlers {
				func(h Handler) {
					defer func() {
						if r := recover(); r != nil {
							log.Error().Interface("recover", r).Msg("event handler panic")
						}
					}()
					h(event)
				}(handler)
			}
		case <-b.quit:
			return
		}
	}
}

// Close shuts down the event bus.
func (b *EventBus) Close() {
	close(b.quit)
	b.wg.Wait()
}
