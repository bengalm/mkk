package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const (
	baseURLLive    = "https://www.okx.com"
	baseURLTestnet = "https://www.okx.com" // demo uses same URL with header
	wsURLLive      = "wss://ws.okx.com:8443/ws/v5/public"
	wsURLPrivate   = "wss://ws.okx.com:8443/ws/v5/private"
)

// Config holds OKX-specific configuration.
type Config struct {
	APIKey     string
	SecretKey  string
	Passphrase string
	Testnet    bool
}

// OKXExchange implements exchange.Exchange for OKX.
type OKXExchange struct {
	config     Config
	httpClient *http.Client
	wsConn     *websocket.Conn
	wsMu       sync.Mutex
	simulated  string // "1" for demo trading
}

// New creates a new OKX exchange client.
func New(cfg Config) (*OKXExchange, error) {
	if err := ValidateAPIKey(cfg.APIKey, cfg.SecretKey, cfg.Passphrase); err != nil {
		return nil, fmt.Errorf("validate OKX credentials: %w", err)
	}
	return &OKXExchange{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		simulated: func() string {
			if cfg.Testnet {
				return "1"
			}
			return "0"
		}(),
	}, nil
}

// OKXResponse is the standard OKX API response wrapper.
type OKXResponse struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// request makes a signed HTTP request to OKX.
func (o *OKXExchange) request(method, path string, body interface{}) (json.RawMessage, error) {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
	}

	bodyStr := string(bodyBytes)
	timestamp := ISOTimestamp()
	signature := Sign(timestamp, method, path, bodyStr, o.config.SecretKey)

	req, err := http.NewRequest(method, baseURLLive+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header = BuildHeaders(o.config.APIKey, o.config.Passphrase, signature, timestamp)
	if o.config.Testnet {
		req.Header.Set("x-simulated-trading", "1")
	}

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var okxResp OKXResponse
	if err := json.Unmarshal(data, &okxResp); err != nil {
		return nil, fmt.Errorf("parse response: %w (body: %s)", err, string(data))
	}

	if okxResp.Code != "0" {
		return nil, fmt.Errorf("OKX error: code=%s msg=%s", okxResp.Code, okxResp.Msg)
	}

	return okxResp.Data, nil
}

// GetTicker fetches current ticker for a pair.
func (o *OKXExchange) GetTicker(pair string) (*exchange.Ticker, error) {
	instID := convertPair(pair)
	data, err := o.request("GET", "/api/v5/market/ticker?instId="+instID, nil)
	if err != nil {
		return nil, err
	}

	var tickers []struct {
		InstID  string `json:"instId"`
		BidPx   string `json:"bidPx"`
		AskPx   string `json:"askPx"`
		Last    string `json:"last"`
		High24h string `json:"high24h"`
		Low24h  string `json:"low24h"`
		Vol24h  string `json:"vol24h"`
		TS      string `json:"ts"`
	}
	if err := json.Unmarshal(data, &tickers); err != nil {
		return nil, fmt.Errorf("parse ticker: %w", err)
	}
	if len(tickers) == 0 {
		return nil, fmt.Errorf("no ticker data for %s", pair)
	}

	t := tickers[0]
	ts, _ := strconv.ParseInt(t.TS, 10, 64)
	return &exchange.Ticker{
		Pair:      pair,
		Bid:       parseFloat(t.BidPx),
		Ask:       parseFloat(t.AskPx),
		Last:      parseFloat(t.Last),
		High24h:   parseFloat(t.High24h),
		Low24h:    parseFloat(t.Low24h),
		Volume24h: parseFloat(t.Vol24h),
		Timestamp: time.UnixMilli(ts),
	}, nil
}

// GetCandles fetches OHLCV candle data.
func (o *OKXExchange) GetCandles(req exchange.CandleRequest) ([]exchange.Candle, error) {
	instID := convertPair(req.Pair)
	bar := req.Timeframe
	path := fmt.Sprintf("/api/v5/market/candles?instId=%s&bar=%s", instID, bar)
	if req.Limit > 0 {
		path += fmt.Sprintf("&limit=%d", req.Limit)
	}

	data, err := o.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	// OKX returns candles as arrays: [ts, o, h, l, c, vol, ...]
	var rawCandles [][]interface{}
	if err := json.Unmarshal(data, &rawCandles); err != nil {
		return nil, fmt.Errorf("parse candles: %w", err)
	}

	candles := make([]exchange.Candle, 0, len(rawCandles))
	for _, rc := range rawCandles {
		if len(rc) < 6 {
			continue
		}
		ts, _ := strconv.ParseInt(fmt.Sprintf("%v", rc[0]), 10, 64)
		candles = append(candles, exchange.Candle{
			Timestamp: time.UnixMilli(ts),
			Open:      parseFloat(fmt.Sprintf("%v", rc[1])),
			High:      parseFloat(fmt.Sprintf("%v", rc[2])),
			Low:       parseFloat(fmt.Sprintf("%v", rc[3])),
			Close:     parseFloat(fmt.Sprintf("%v", rc[4])),
			Volume:    parseFloat(fmt.Sprintf("%v", rc[5])),
			Timeframe: req.Timeframe,
		})
	}

	return candles, nil
}

// GetOrderBook fetches the order book.
func (o *OKXExchange) GetOrderBook(pair string, depth int) (*exchange.OrderBook, error) {
	instID := convertPair(pair)
	if depth <= 0 {
		depth = 20
	}
	path := fmt.Sprintf("/api/v5/market/books?instId=%s&sz=%d", instID, depth)

	data, err := o.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var books []struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
		TS   string     `json:"ts"`
	}
	if err := json.Unmarshal(data, &books); err != nil {
		return nil, fmt.Errorf("parse orderbook: %w", err)
	}
	if len(books) == 0 {
		return nil, fmt.Errorf("no orderbook data for %s", pair)
	}

	b := books[0]
	ob := &exchange.OrderBook{Pair: pair}
	for _, ask := range b.Asks {
		if len(ask) >= 2 {
			ob.Asks = append(ob.Asks, exchange.PriceLevel{
				Price:  parseFloat(ask[0]),
				Amount: parseFloat(ask[1]),
			})
		}
	}
	for _, bid := range b.Bids {
		if len(bid) >= 2 {
			ob.Bids = append(ob.Bids, exchange.PriceLevel{
				Price:  parseFloat(bid[0]),
				Amount: parseFloat(bid[1]),
			})
		}
	}
	ts, _ := strconv.ParseInt(b.TS, 10, 64)
	ob.Timestamp = time.UnixMilli(ts)
	return ob, nil
}

// GetBalance fetches account balances.
func (o *OKXExchange) GetBalance(currencies ...string) ([]exchange.Balance, error) {
	data, err := o.request("GET", "/api/v5/account/balance", nil)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		Details []struct {
			Ccy     string `json:"ccy"`
			AvailBal string `json:"availBal"`
			Frozen   string `json:"frozen"`
			Eq       string `json:"eq"`
		} `json:"details"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parse balance: %w", err)
	}

	balances := make([]exchange.Balance, 0)
	for _, r := range resp {
		for _, d := range r.Details {
			if len(currencies) > 0 && !contains(currencies, d.Ccy) {
				continue
			}
			avail := parseFloat(d.AvailBal)
			frozen := parseFloat(d.Frozen)
			if avail == 0 && frozen == 0 {
				continue
			}
			balances = append(balances, exchange.Balance{
				Currency:  d.Ccy,
				Available: avail,
				Frozen:    frozen,
				Total:     parseFloat(d.Eq),
			})
		}
	}
	return balances, nil
}

// GetPositions fetches open positions.
func (o *OKXExchange) GetPositions(pairs ...string) ([]exchange.Position, error) {
	path := "/api/v5/account/positions"
	if len(pairs) > 0 {
		instIDs := make([]string, len(pairs))
		for i, p := range pairs {
			instIDs[i] = convertPair(p)
		}
		path += "?instId=" + instIDs[0]
	}

	data, err := o.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var positions []struct {
		InstID  string `json:"instId"`
		PosSide string `json:"posSide"`
		Pos     string `json:"pos"`
		AvgPx   string `json:"avgPx"`
		MarkPx  string `json:"markPx"`
		Upl     string `json:"upl"`
		Lever   string `json:"lever"`
		Margin  string `json:"margin"`
		CTime   string `json:"cTime"`
	}
	if err := json.Unmarshal(data, &positions); err != nil {
		return nil, fmt.Errorf("parse positions: %w", err)
	}

	result := make([]exchange.Position, 0, len(positions))
	for _, p := range positions {
		size := parseFloat(p.Pos)
		if size == 0 {
			continue
		}
		side := exchange.Buy
		if p.PosSide == "short" {
			side = exchange.Sell
		}
		ct, _ := strconv.ParseInt(p.CTime, 10, 64)
		result = append(result, exchange.Position{
			Pair:          convertInstID(p.InstID),
			Side:          side,
			Size:          size,
			EntryPrice:    parseFloat(p.AvgPx),
			MarkPrice:     parseFloat(p.MarkPx),
			UnrealizedPnL: parseFloat(p.Upl),
			Leverage:      parseInt(p.Lever),
			Margin:        parseFloat(p.Margin),
			Timestamp:     time.UnixMilli(ct),
		})
	}
	return result, nil
}

// PlaceOrder places a new order.
func (o *OKXExchange) PlaceOrder(req exchange.OrderRequest) (*exchange.Order, error) {
	instID := convertPair(req.Pair)
	side := string(req.Side)
	if side == "buy" {
		side = "buy"
	}
_ordType := string(req.Type)

	body := map[string]interface{}{
		"instId":  instID,
		"tdMode":  "cross", // cross margin
		"side":    side,
		"ordType": _ordType,
		"sz":      fmt.Sprintf("%.8f", req.Amount),
	}
	if req.Price > 0 {
		body["px"] = fmt.Sprintf("%.8f", req.Price)
	}
	if req.ClientID != "" {
		body["clOrdId"] = req.ClientID
	}

	data, err := o.request("POST", "/api/v5/trade/order", body)
	if err != nil {
		return nil, err
	}

	var result []struct {
		OrdID   string `json:"ordId"`
		ClOrdID string `json:"clOrdId"`
		SCode   string `json:"sCode"`
		SMsg    string `json:"sMsg"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("parse order response: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty order response")
	}
	r := result[0]
	if r.SCode != "0" {
		return nil, fmt.Errorf("order rejected: %s", r.SMsg)
	}

	return &exchange.Order{
		ID:       r.OrdID,
		ClientID: r.ClOrdID,
		Pair:     req.Pair,
		Side:     req.Side,
		Type:     req.Type,
		Price:    req.Price,
		Amount:   req.Amount,
		Status:   exchange.StatusNew,
	}, nil
}

// CancelOrder cancels an existing order.
func (o *OKXExchange) CancelOrder(pair, orderID string) error {
	instID := convertPair(pair)
	body := map[string]interface{}{
		"instId": instID,
		"ordId":  orderID,
	}

	_, err := o.request("POST", "/api/v5/trade/cancel-order", body)
	return err
}

// GetOrder fetches a single order by ID.
func (o *OKXExchange) GetOrder(pair, orderID string) (*exchange.Order, error) {
	instID := convertPair(pair)
	path := fmt.Sprintf("/api/v5/trade/order?instId=%s&ordId=%s", instID, orderID)

	data, err := o.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var orders []struct {
		OrdID    string `json:"ordId"`
		ClOrdID  string `json:"clOrdId"`
		InstID   string `json:"instId"`
		Side     string `json:"side"`
		OrdType  string `json:"ordType"`
		Px       string `json:"px"`
		Sz       string `json:"sz"`
		FillSz   string `json:"fillSz"`
		State    string `json:"state"`
		CTime    string `json:"cTime"`
	}
	if err := json.Unmarshal(data, &orders); err != nil {
		return nil, fmt.Errorf("parse order: %w", err)
	}
	if len(orders) == 0 {
		return nil, fmt.Errorf("order not found: %s", orderID)
	}

	ord := orders[0]
	ct, _ := strconv.ParseInt(ord.CTime, 10, 64)
	return &exchange.Order{
		ID:        ord.OrdID,
		ClientID:  ord.ClOrdID,
		Pair:      convertInstID(ord.InstID),
		Side:      exchange.OrderSide(ord.Side),
		Type:      exchange.OrderType(ord.OrdType),
		Price:     parseFloat(ord.Px),
		Amount:    parseFloat(ord.Sz),
		Filled:    parseFloat(ord.FillSz),
		Status:    convertStatus(ord.State),
		Timestamp: time.UnixMilli(ct),
	}, nil
}

// GetOpenOrders fetches all open orders for a pair.
func (o *OKXExchange) GetOpenOrders(pair string) ([]exchange.Order, error) {
	instID := convertPair(pair)
	path := fmt.Sprintf("/api/v5/trade/orders-pending?instId=%s", instID)

	data, err := o.request("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var orders []struct {
		OrdID   string `json:"ordId"`
		ClOrdID string `json:"clOrdId"`
		InstID  string `json:"instId"`
		Side    string `json:"side"`
		OrdType string `json:"ordType"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		FillSz  string `json:"fillSz"`
		State   string `json:"state"`
		CTime   string `json:"cTime"`
	}
	if err := json.Unmarshal(data, &orders); err != nil {
		return nil, fmt.Errorf("parse orders: %w", err)
	}

	result := make([]exchange.Order, 0, len(orders))
	for _, ord := range orders {
		ct, _ := strconv.ParseInt(ord.CTime, 10, 64)
		result = append(result, exchange.Order{
			ID:        ord.OrdID,
			ClientID:  ord.ClOrdID,
			Pair:      convertInstID(ord.InstID),
			Side:      exchange.OrderSide(ord.Side),
			Type:      exchange.OrderType(ord.OrdType),
			Price:     parseFloat(ord.Px),
			Amount:    parseFloat(ord.Sz),
			Filled:    parseFloat(ord.FillSz),
			Status:    convertStatus(ord.State),
			Timestamp: time.UnixMilli(ct),
		})
	}
	return result, nil
}

// SubscribeTicker subscribes to real-time ticker updates via WebSocket.
func (o *OKXExchange) SubscribeTicker(pair string, handler func(exchange.Ticker)) error {
	instID := convertPair(pair)
	return o.subscribePublic("tickers", instID, func(data json.RawMessage) {
		var tick struct {
			InstID  string `json:"instId"`
			BidPx   string `json:"bidPx"`
			AskPx   string `json:"askPx"`
			Last    string `json:"last"`
			High24h string `json:"high24h"`
			Low24h  string `json:"low24h"`
			Vol24h  string `json:"vol24h"`
			TS      string `json:"ts"`
		}
		if err := json.Unmarshal(data, &tick); err != nil {
			return
		}
		ts, _ := strconv.ParseInt(tick.TS, 10, 64)
		handler(exchange.Ticker{
			Pair:      pair,
			Bid:       parseFloat(tick.BidPx),
			Ask:       parseFloat(tick.AskPx),
			Last:      parseFloat(tick.Last),
			High24h:   parseFloat(tick.High24h),
			Low24h:    parseFloat(tick.Low24h),
			Volume24h: parseFloat(tick.Vol24h),
			Timestamp: time.UnixMilli(ts),
		})
	})
}

// SubscribeCandles subscribes to real-time candle updates.
func (o *OKXExchange) SubscribeCandles(pair, timeframe string, handler func(exchange.Candle)) error {
	instID := convertPair(pair)
	channel := fmt.Sprintf("candle%s", timeframe)
	return o.subscribePublic(channel, instID, func(data json.RawMessage) {
		var raw []interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			return
		}
		if len(raw) < 6 {
			return
		}
		ts, _ := strconv.ParseInt(fmt.Sprintf("%v", raw[0]), 10, 64)
		handler(exchange.Candle{
			Timestamp: time.UnixMilli(ts),
			Open:      parseFloat(fmt.Sprintf("%v", raw[1])),
			High:      parseFloat(fmt.Sprintf("%v", raw[2])),
			Low:       parseFloat(fmt.Sprintf("%v", raw[3])),
			Close:     parseFloat(fmt.Sprintf("%v", raw[4])),
			Volume:    parseFloat(fmt.Sprintf("%v", raw[5])),
			Timeframe: timeframe,
		})
	})
}

// SubscribeOrderBook subscribes to real-time order book updates.
func (o *OKXExchange) SubscribeOrderBook(pair string, handler func(exchange.OrderBook)) error {
	instID := convertPair(pair)
	return o.subscribePublic("books5", instID, func(data json.RawMessage) {
		var books []struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
			TS   string     `json:"ts"`
		}
		if err := json.Unmarshal(data, &books); err != nil {
			return
		}
		if len(books) == 0 {
			return
		}
		b := books[0]
		ob := exchange.OrderBook{Pair: pair}
		for _, ask := range b.Asks {
			if len(ask) >= 2 {
				ob.Asks = append(ob.Asks, exchange.PriceLevel{Price: parseFloat(ask[0]), Amount: parseFloat(ask[1])})
			}
		}
		for _, bid := range b.Bids {
			if len(bid) >= 2 {
				ob.Bids = append(ob.Bids, exchange.PriceLevel{Price: parseFloat(bid[0]), Amount: parseFloat(bid[1])})
			}
		}
		ts, _ := strconv.ParseInt(b.TS, 10, 64)
		ob.Timestamp = time.UnixMilli(ts)
		handler(ob)
	})
}

// subscribePublic connects to OKX public WebSocket and subscribes to a channel.
func (o *OKXExchange) subscribePublic(channel, instID string, handler func(json.RawMessage)) error {
	o.wsMu.Lock()
	defer o.wsMu.Unlock()

	if o.wsConn == nil {
		conn, _, err := websocket.DefaultDialer.Dial(wsURLLive, nil)
		if err != nil {
			return fmt.Errorf("connect to OKX WS: %w", err)
		}
		o.wsConn = conn

		// Read loop
		go func() {
			defer o.wsConn.Close()
			for {
				_, msg, err := o.wsConn.ReadMessage()
				if err != nil {
					log.Error().Err(err).Msg("OKX WS read error")
					return
				}
				var wsMsg struct {
					Arg struct {
						Channel string `json:"channel"`
						InstID  string `json:"instId"`
					} `json:"arg"`
					Data json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(msg, &wsMsg); err != nil {
					continue
				}
				if wsMsg.Data != nil {
					handler(wsMsg.Data)
				}
			}
		}()
	}

	sub := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{"channel": channel, "instId": instID},
		},
	}
	return o.wsConn.WriteJSON(sub)
}

// Close shuts down the exchange client.
func (o *OKXExchange) Close() error {
	if o.wsConn != nil {
		return o.wsConn.Close()
	}
	o.httpClient.CloseIdleConnections()
	return nil
}

// Helper: convert "BTC-USDT" → "BTC-USDT" (OKX uses same format)
func convertPair(pair string) string {
	return strings.ReplaceAll(pair, "/", "-")
}

// Helper: convert "BTC-USDT" back to "BTC-USDT"
func convertInstID(instID string) string {
	return instID // same format
}

func convertStatus(state string) exchange.OrderStatus {
	switch state {
	case "live":
		return exchange.StatusNew
	case "partially_filled":
		return exchange.StatusPartiallyFilled
	case "filled":
		return exchange.StatusFilled
	case "canceled":
		return exchange.StatusCanceled
	default:
		return exchange.StatusRejected
	}
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// Ensure OKXExchange implements Exchange interface at compile time.
var _ exchange.Exchange = (*OKXExchange)(nil)

// contextKey for storing context values
type contextKey string

// unused import guard
var _ context.Context
