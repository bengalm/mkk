package exchange

import "time"

// OrderSide represents buy or sell.
type OrderSide string

const (
	Buy  OrderSide = "buy"
	Sell OrderSide = "sell"
)

// OrderType represents order types.
type OrderType string

const (
	OrderMarket          OrderType = "market"
	OrderLimit           OrderType = "limit"
	OrderStopMarket      OrderType = "stop_market"
	OrderTakeProfitMarket OrderType = "take_profit_market"
)

// OrderStatus represents order lifecycle.
type OrderStatus string

const (
	StatusNew            OrderStatus = "new"
	StatusPartiallyFilled OrderStatus = "partially_filled"
	StatusFilled         OrderStatus = "filled"
	StatusCanceled       OrderStatus = "canceled"
	StatusRejected       OrderStatus = "rejected"
)

// Pair represents a trading pair.
type Pair struct {
	Base   string `json:"base"`   // e.g., "BTC"
	Quote  string `json:"quote"`  // e.g., "USDT"
	Symbol string `json:"symbol"` // e.g., "BTC-USDT"
}

// NewPair creates a Pair from base and quote.
func NewPair(base, quote string) Pair {
	return Pair{Base: base, Quote: quote, Symbol: base + "-" + quote}
}

// Order represents a trading order.
type Order struct {
	ID        string      `json:"id"`
	ClientID  string      `json:"client_id"`
	Pair      string      `json:"pair"`
	Side      OrderSide   `json:"side"`
	Type      OrderType   `json:"type"`
	Price     float64     `json:"price"`
	Amount    float64     `json:"amount"`
	Filled    float64     `json:"filled"`
	Remaining float64     `json:"remaining"`
	Status    OrderStatus `json:"status"`
	Timestamp time.Time   `json:"timestamp"`
}

// Trade represents a completed trade/fill.
type Trade struct {
	ID        string    `json:"id"`
	OrderID   string    `json:"order_id"`
	Pair      string    `json:"pair"`
	Side      OrderSide `json:"side"`
	Price     float64   `json:"price"`
	Amount    float64   `json:"amount"`
	Fee       float64   `json:"fee"`
	FeeCurrency string  `json:"fee_currency"`
	Timestamp time.Time `json:"timestamp"`
}

// Position represents an open position.
type Position struct {
	ID            string    `json:"id"`
	Pair          string    `json:"pair"`
	Side          OrderSide `json:"side"`
	Size          float64   `json:"size"`
	EntryPrice    float64   `json:"entry_price"`
	MarkPrice     float64   `json:"mark_price"`
	UnrealizedPnL float64   `json:"unrealized_pnl"`
	Leverage      int       `json:"leverage"`
	Margin        float64   `json:"margin"`
	Timestamp     time.Time `json:"timestamp"`
}

// Ticker represents current market data.
type Ticker struct {
	Pair      string    `json:"pair"`
	Bid       float64   `json:"bid"`
	Ask       float64   `json:"ask"`
	Last      float64   `json:"last"`
	High24h   float64   `json:"high_24h"`
	Low24h    float64   `json:"low_24h"`
	Volume24h float64   `json:"volume_24h"`
	Timestamp time.Time `json:"timestamp"`
}

// Candle represents OHLCV data.
type Candle struct {
	Pair      string    `json:"pair"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
	Timeframe string    `json:"timeframe"`
}

// PriceLevel represents a single level in the order book.
type PriceLevel struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

// OrderBook represents the exchange order book.
type OrderBook struct {
	Pair      string       `json:"pair"`
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
	Timestamp time.Time    `json:"timestamp"`
}

// Balance represents a single currency balance.
type Balance struct {
	Currency  string  `json:"currency"`
	Available float64 `json:"available"`
	Frozen    float64 `json:"frozen"`
	Total     float64 `json:"total"`
}

// AccountInfo holds account balances and positions.
type AccountInfo struct {
	Balances  []Balance  `json:"balances"`
	Positions []Position `json:"positions"`
}

// OrderRequest is used to place a new order.
type OrderRequest struct {
	Pair       string    `json:"pair"`
	Side       OrderSide `json:"side"`
	Type       OrderType `json:"type"`
	Price      float64   `json:"price,omitempty"`
	Amount     float64   `json:"amount"`
	StopPrice  float64   `json:"stop_price,omitempty"`
	ReduceOnly bool      `json:"reduce_only,omitempty"`
	ClientID   string    `json:"client_id,omitempty"`
	TdMode     string    `json:"td_mode,omitempty"`  // "cross", "isolated", "cash"
	PosSide    string    `json:"pos_side,omitempty"` // "long", "short", "net" (for hedge mode)
}

// CandleRequest is used to request candle data.
type CandleRequest struct {
	Pair      string `json:"pair"`
	Timeframe string `json:"timeframe"` // "1m", "5m", "15m", "1h", "4h", "1d"
	Start     string `json:"start,omitempty"`
	End       string `json:"end,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

// Exchange is the common interface for all exchange implementations.
type Exchange interface {
	// Market data
	GetTicker(pair string) (*Ticker, error)
	GetCandles(req CandleRequest) ([]Candle, error)
	GetOrderBook(pair string, depth int) (*OrderBook, error)

	// Account
	GetBalance(currencies ...string) ([]Balance, error)
	GetPositions(pairs ...string) ([]Position, error)

	// Trading
	PlaceOrder(req OrderRequest) (*Order, error)
	BatchPlaceOrders(reqs []OrderRequest) ([]*Order, error)
	CancelOrder(pair, orderID string) error
	GetOrder(pair, orderID string) (*Order, error)
	GetOpenOrders(pair string) ([]Order, error)

	// WebSocket subscriptions
	SubscribeTicker(pair string, handler func(Ticker)) error
	SubscribeCandles(pair, timeframe string, handler func(Candle)) error
	SubscribeOrderBook(pair string, handler func(OrderBook)) error
	SubscribeOrders(handler func(Trade)) error

	// Lifecycle
	Close() error
}
