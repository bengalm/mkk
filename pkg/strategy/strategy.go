package strategy

import (
	"fmt"
	"strings"
	"sync"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/rs/zerolog/log"
)

// SignalAction defines what the strategy wants to do.
type SignalAction string

const (
	ActionBuy       SignalAction = "buy"
	ActionSell      SignalAction = "sell"
	ActionHold      SignalAction = "hold"
	ActionCloseLong SignalAction = "close_long"
	ActionCloseShort SignalAction = "close_short"
)

// TradeSignal is emitted by a strategy when it wants to trade.
type TradeSignal struct {
	Action     SignalAction       `json:"action"`
	Pair       string             `json:"pair"`
	Price      float64            `json:"price"`
	Amount     float64            `json:"amount"`
	Type       exchange.OrderType `json:"type"`
	StopLoss   float64            `json:"stop_loss,omitempty"`
	TakeProfit float64            `json:"take_profit,omitempty"`
	Reason     string             `json:"reason"`
	Strategy   string             `json:"strategy"`
}

// Strategy is the interface all strategies must implement.
type Strategy interface {
	// Name returns the strategy name.
	Name() string
	// Init initializes the strategy with config and exchange.
	Init(config map[string]interface{}, ex exchange.Exchange, bus *eventbus.EventBus) error
	// OnTick is called on every ticker update.
	OnTick(ticker exchange.Ticker)
	// OnCandle is called on every new candle.
	OnCandle(candle exchange.Candle)
	// OnFill is called when an order is filled.
	OnFill(trade exchange.Trade)
	// Stop cleanly shuts down the strategy.
	Stop()
	// IsActive returns whether the strategy is running.
	IsActive() bool
	// Stats returns current strategy statistics.
	Stats() map[string]interface{}
}

// BaseStrategy provides common functionality for all strategies.
type BaseStrategy struct {
	name     string
	exchange exchange.Exchange
	bus      *eventbus.EventBus
	active   bool
	mu       sync.RWMutex
	signals  chan TradeSignal
}

// InitBase initializes base strategy fields.
func (b *BaseStrategy) InitBase(name string, ex exchange.Exchange, bus *eventbus.EventBus) {
	b.name = name
	b.exchange = ex
	b.bus = bus
	b.active = true
	b.signals = make(chan TradeSignal, 100)
}

// Name returns the strategy name.
func (b *BaseStrategy) Name() string { return b.name }

// IsActive returns whether the strategy is running.
func (b *BaseStrategy) IsActive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.active
}

// Stop marks the strategy as inactive.
func (b *BaseStrategy) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = false
	close(b.signals)
}

// Exchange returns the exchange client.
func (b *BaseStrategy) Exchange() exchange.Exchange { return b.exchange }

// GetBus returns the event bus.
func (b *BaseStrategy) GetBus() *eventbus.EventBus { return b.bus }

// Signals returns the signal channel.
func (b *BaseStrategy) Signals() <-chan TradeSignal { return b.signals }

// EmitSignal publishes a trade signal.
func (b *BaseStrategy) EmitSignal(signal TradeSignal) {
	signal.Strategy = b.name
	select {
	case b.signals <- signal:
	default:
		log.Warn().Str("strategy", b.name).Msg("signal channel full, dropping")
	}
	if b.bus != nil {
		b.bus.Publish(eventbus.TopicStrategy, signal)
	}
}

// GetFloatParam extracts a float parameter from config.
func GetFloatParam(config map[string]interface{}, key string, defaultVal float64) float64 {
	val, ok := config[key]
	if !ok {
		return defaultVal
	}
	switch v := val.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		if f != 0 {
			return f
		}
	}
	return defaultVal
}

// GetIntParam extracts an int parameter from config.
func GetIntParam(config map[string]interface{}, key string, defaultVal int) int {
	val, ok := config[key]
	if !ok {
		return defaultVal
	}
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return defaultVal
}

// GetStringParam extracts a string parameter from config.
func GetStringParam(config map[string]interface{}, key string, defaultVal string) string {
	val, ok := config[key]
	if !ok {
		return defaultVal
	}
	if s, ok := val.(string); ok {
		return s
	}
	return defaultVal
}

// GetBoolParam extracts a bool parameter from config.
func GetBoolParam(config map[string]interface{}, key string, defaultVal bool) bool {
	val, ok := config[key]
	if !ok {
		return defaultVal
	}
	if b, ok := val.(bool); ok {
		return b
	}
	// Handle YAML "1"/"0" or "true"/"false" strings
	if s, ok := val.(string); ok {
		return strings.EqualFold(s, "true") || s == "1"
	}
	return defaultVal
}

// Registry holds all registered strategy constructors.
var registry = make(map[string]func() Strategy)

// Register registers a strategy constructor.
func Register(name string, constructor func() Strategy) {
	registry[name] = constructor
	log.Debug().Str("name", name).Msg("strategy registered")
}

// Get creates a new strategy by name.
func Get(name string) (Strategy, error) {
	constructor, ok := registry[name]
	if !ok {
		available := make([]string, 0, len(registry))
		for k := range registry {
			available = append(available, k)
		}
		return nil, fmt.Errorf("strategy %q not found, available: %v", name, available)
	}
	return constructor(), nil
}

// List returns all registered strategy names.
func List() []string {
	names := make([]string, 0, len(registry))
	for k := range registry {
		names = append(names, k)
	}
	return names
}

// Manager manages multiple running strategies.
type Manager struct {
	strategies map[string]Strategy
	mu         sync.RWMutex
}

// NewManager creates a new strategy manager.
func NewManager() *Manager {
	return &Manager{
		strategies: make(map[string]Strategy),
	}
}

// Add adds and initializes a strategy.
func (m *Manager) Add(s Strategy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.strategies[s.Name()] = s
	return nil
}

// Get retrieves a strategy by name.
func (m *Manager) Get(name string) (Strategy, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.strategies[name]
	return s, ok
}

// StopAll stops all running strategies.
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.strategies {
		s.Stop()
	}
}

// List returns all active strategies.
func (m *Manager) List() []Strategy {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Strategy, 0, len(m.strategies))
	for _, s := range m.strategies {
		result = append(result, s)
	}
	return result
}
