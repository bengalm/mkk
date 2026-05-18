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
	config      Config
	httpClient  *http.Client
	wsConn      *websocket.Conn        // public WS
	wsPrivConn  *websocket.Conn        // private WS
	wsMu        sync.Mutex
	wsPrivMu      sync.Mutex
	simulated     string                 // "1" for demo trading
	handlers      map[string]func(json.RawMessage) // public WS channel→handler
	orderHandlers   []func(exchange.Trade)            // private WS order fill handlers
	orderMu         sync.Mutex                         // protects orderHandlers + dispatchedOrders
	dispatchedOrders map[string]bool                   // dedup: orderID → dispatched
	wsDone          chan struct{}                       // signal to stop WS read loops
	// Fill reconciliation
	reconcileMu     sync.Mutex
	lastReconcileTS int64 // unix seconds of last successful reconciliation
}

// New creates a new OKX exchange client.
func New(cfg Config) (*OKXExchange, error) {
	if err := ValidateAPIKey(cfg.APIKey, cfg.SecretKey, cfg.Passphrase); err != nil {
		return nil, fmt.Errorf("validate OKX credentials: %w", err)
	}
	return &OKXExchange{
		config:     cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		simulated: func() string {
			if cfg.Testnet {
				return "1"
			}
			return "0"
		}(),
		handlers:          make(map[string]func(json.RawMessage)),
		dispatchedOrders:  make(map[string]bool),
		wsDone:            make(chan struct{}),
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

	// OKX returns candles newest-first; reverse to oldest-first for indicators
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
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

// SetLeverage sets leverage for a swap instrument (isolated mode).
// It accepts a pair name (e.g. "SOL-USDT-SWAP") and converts it internally.
func (o *OKXExchange) SetLeverage(pair string, lever int, mgnMode string) error {
	instID := convertPair(pair)
	if mgnMode == "" {
		mgnMode = "isolated"
	}
	body := map[string]interface{}{
		"instId":  instID,
		"lever":   fmt.Sprintf("%d", lever),
		"mgnMode": mgnMode,
	}
	_, err := o.request("POST", "/api/v5/account/set-leverage", body)
	return err
}

// PlaceAlgoOrder places an algo order (stop-loss, take-profit, oco, etc).
func (o *OKXExchange) PlaceAlgoOrder(params map[string]interface{}) (string, error) {
	data, err := o.request("POST", "/api/v5/trade/order-algo", params)
	if err != nil {
		return "", err
	}
	var result []struct {
		AlgoID string `json:"algoId"`
		SCode  string `json:"sCode"`
		SMsg   string `json:"sMsg"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return "", fmt.Errorf("parse algo order response: %w", err)
	}
	if len(result) == 0 {
		return "", fmt.Errorf("empty algo order response")
	}
	if result[0].SCode != "0" {
		return "", fmt.Errorf("algo order rejected: %s", result[0].SMsg)
	}
	return result[0].AlgoID, nil
}

// PlaceOrder places a new order.
func (o *OKXExchange) PlaceOrder(req exchange.OrderRequest) (*exchange.Order, error) {
	instID := convertPair(req.Pair)
	side := string(req.Side)
	if side == "buy" {
		side = "buy"
	}
	_ordType := string(req.Type)

	tdMode := "cross"
	posSide := "net"
	if req.TdMode != "" {
		tdMode = req.TdMode
	} else if strings.HasSuffix(instID, "-SWAP") || strings.HasSuffix(instID, "-FUTURES") {
		tdMode = "isolated"
	}
	if req.PosSide != "" {
		posSide = req.PosSide
	}

	body := map[string]interface{}{
		"instId":  instID,
		"tdMode":  tdMode,
		"side":    side,
		"ordType": _ordType,
		"sz":      fmt.Sprintf("%.8f", req.Amount),
		"posSide": posSide,
	}
	if req.Price > 0 {
		body["px"] = fmt.Sprintf("%.8f", req.Price)
	}
	if req.ClientID != "" {
		body["clOrdId"] = req.ClientID
	}
	if req.ReduceOnly {
		body["reduceOnly"] = true
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

// BatchPlaceOrders places up to 20 orders in a single API call.
func (o *OKXExchange) BatchPlaceOrders(reqs []exchange.OrderRequest) ([]*exchange.Order, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	// OKX allows max 20 orders per batch
	if len(reqs) > 20 {
		reqs = reqs[:20]
	}

	orders := make([]map[string]interface{}, 0, len(reqs))
	for _, req := range reqs {
		instID := convertPair(req.Pair)
		tdMode := "cross"
		posSide := "net"
		if req.TdMode != "" {
			tdMode = req.TdMode
		} else if strings.HasSuffix(instID, "-SWAP") || strings.HasSuffix(instID, "-FUTURES") {
			tdMode = "isolated"
		}
		if req.PosSide != "" {
			posSide = req.PosSide
		}
		item := map[string]interface{}{
			"instId":  instID,
			"tdMode":  tdMode,
			"side":    string(req.Side),
			"ordType": string(req.Type),
			"sz":      fmt.Sprintf("%.8f", req.Amount),
			"posSide": posSide,
		}
		if req.Price > 0 {
			item["px"] = fmt.Sprintf("%.8f", req.Price)
		}
		if req.ClientID != "" {
			item["clOrdId"] = req.ClientID
		}
		if req.ReduceOnly {
			item["reduceOnly"] = true
		}
		orders = append(orders, item)
	}

	data, err := o.request("POST", "/api/v5/trade/batch-orders", orders)
	if err != nil {
		return nil, fmt.Errorf("batch order failed: %w", err)
	}

	var results []struct {
		OrdID   string `json:"ordId"`
		ClOrdID string `json:"clOrdId"`
		SCode   string `json:"sCode"`
		SMsg    string `json:"sMsg"`
	}
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("parse batch response: %w", err)
	}

	out := make([]*exchange.Order, 0, len(results))
	for i, r := range results {
		if r.SCode != "0" {
			log.Warn().Str("scode", r.SCode).Str("smsg", r.SMsg).Int("index", i).Msg("batch order item rejected")
			continue
		}
		req := reqs[i]
		out = append(out, &exchange.Order{
			ID:       r.OrdID,
			Pair:     req.Pair,
			Side:     req.Side,
			Type:     req.Type,
			Price:    req.Price,
			Amount:   req.Amount,
			Status:   exchange.StatusNew,
		})
	}
	return out, nil
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
// Supports multiple handlers per channel with automatic reconnection.
func (o *OKXExchange) subscribePublic(channel, instID string, handler func(json.RawMessage)) error {
	o.wsMu.Lock()
	defer o.wsMu.Unlock()

	// Store handler by channel+instID key
	handlerKey := channel + ":" + instID
	o.handlers[handlerKey] = handler

	if o.wsConn == nil {
		conn, _, err := websocket.DefaultDialer.Dial(wsURLLive, nil)
		if err != nil {
			return fmt.Errorf("connect to OKX public WS: %w", err)
		}
		o.wsConn = conn

		// Read loop with reconnection
		go o.publicWSReadLoop()
	}

	// Subscribe
	sub := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{"channel": channel, "instId": instID},
		},
	}
	return o.wsConn.WriteJSON(sub)
}

// publicWSReadLoop reads messages from public WS and dispatches to handlers.
func (o *OKXExchange) publicWSReadLoop() {
	for {
		select {
		case <-o.wsDone:
			return
		default:
		}

		_, msg, err := o.wsConn.ReadMessage()
		if err != nil {
			log.Warn().Err(err).Msg("OKX public WS disconnected, reconnecting...")
			o.reconnectPublicWS()
			continue
		}

		// Handle ping/pong
		if string(msg) == "pong" {
			continue
		}

		var wsMsg struct {
			Arg  struct {
				Channel string `json:"channel"`
				InstID  string `json:"instId"`
			} `json:"arg"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			continue
		}
		if wsMsg.Data == nil {
			continue
		}

		handlerKey := wsMsg.Arg.Channel + ":" + wsMsg.Arg.InstID
		o.wsMu.Lock()
		h, ok := o.handlers[handlerKey]
		o.wsMu.Unlock()
		if ok {
			h(wsMsg.Data)
		}
	}
}

// reconnectPublicWS reconnects the public WebSocket and re-subscribes all channels.
func (o *OKXExchange) reconnectPublicWS() {
	o.wsMu.Lock()
	defer o.wsMu.Unlock()

	for i := 0; i < 5; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURLLive, nil)
		if err != nil {
			log.Error().Err(err).Int("attempt", i+1).Msg("Public WS reconnect failed")
			time.Sleep(time.Duration(i+1) * 2 * time.Second)
			continue
		}
		o.wsConn = conn

		// Re-subscribe all stored handlers
		for key, _ := range o.handlers {
			parts := strings.SplitN(key, ":", 2)
			if len(parts) != 2 {
				continue
			}
			sub := map[string]interface{}{
				"op": "subscribe",
				"args": []map[string]string{
					{"channel": parts[0], "instId": parts[1]},
				},
			}
			if err := o.wsConn.WriteJSON(sub); err != nil {
				log.Error().Err(err).Str("key", key).Msg("Re-subscribe failed")
			}
		}
		log.Info().Int("handlers", len(o.handlers)).Msg("Public WS reconnected")
		return
	}
	log.Error().Msg("Public WS reconnect exhausted all retries")
}

// SubscribeOrders subscribes to private order updates via OKX private WebSocket.
// SubscribeOrders subscribes to OKX private WS for order fill events.
func (o *OKXExchange) SubscribeOrders(handler func(exchange.Trade)) error {
	o.orderMu.Lock()
	o.orderHandlers = append(o.orderHandlers, handler)
	o.orderMu.Unlock()

	// If private WS already connected, just add handler
	o.wsPrivMu.Lock()
	if o.wsPrivConn != nil {
		o.wsPrivMu.Unlock()
		return nil
	}
	o.wsPrivMu.Unlock()

	// Dial private WS
	conn, _, err := websocket.DefaultDialer.Dial(wsURLPrivate, nil)
	if err != nil {
		return fmt.Errorf("private WS dial: %w", err)
	}

	// Login
	ts := WSTimestamp()
	sign := Sign(ts, "GET", "/users/self/verify", "", o.config.SecretKey)
	loginMsg := map[string]interface{}{
		"op": "login",
		"args": []map[string]string{
			{
				"apiKey":     o.config.APIKey,
				"passphrase": o.config.Passphrase,
				"timestamp":  ts,
				"sign":       sign,
			},
		},
	}
	if err := conn.WriteJSON(loginMsg); err != nil {
		conn.Close()
		return fmt.Errorf("WS login write: %w", err)
	}

	// Wait for login response
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return fmt.Errorf("WS login read: %w", err)
	}
	var loginResp struct {
		Event string `json:"event"`
		Msg   string `json:"msg"`
	}
	json.Unmarshal(msg, &loginResp)
	if loginResp.Event == "error" {
		conn.Close()
		return fmt.Errorf("WS login failed: %s", loginResp.Msg)
	}
	log.Info().Msg("OKX private WS authenticated")

	// Subscribe to orders + account channels
	subMsg := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{"channel": "orders", "instType": "SWAP"},
			{"channel": "account"},
		},
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		conn.Close()
		return fmt.Errorf("WS subscribe orders: %w", err)
	}

	o.wsPrivMu.Lock()
	o.wsPrivConn = conn
	o.wsPrivMu.Unlock()

	// Read loop in goroutine
	go o.privateWSReadLoop()

	// Fill reconciliation loop
	go o.reconciliationLoop()

	log.Info().Msg("Subscribed to OKX order updates via private WS")
	return nil
}

// dispatchFill sends a fill trade to all registered handlers.
func (o *OKXExchange) dispatchFill(trade exchange.Trade) {
	o.orderMu.Lock()
	// Dedup: skip if this order was already dispatched
	if o.dispatchedOrders[trade.OrderID] {
		o.orderMu.Unlock()
		log.Debug().Str("order_id", trade.OrderID).Msg("Fill dedup: skipping already dispatched")
		return
	}
	o.dispatchedOrders[trade.OrderID] = true
	handlers := make([]func(exchange.Trade), len(o.orderHandlers))
	copy(handlers, o.orderHandlers)
	o.orderMu.Unlock()

	for _, h := range handlers {
		h(trade)
	}
}

// privateWSReadLoop reads from private WS with proper blocking + ping.
func (o *OKXExchange) privateWSReadLoop() {
	// Ping goroutine — sends keepalive every 25s
	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.wsPrivMu.Lock()
				if o.wsPrivConn != nil {
					o.wsPrivConn.WriteMessage(websocket.TextMessage, []byte("ping"))
				}
				o.wsPrivMu.Unlock()
			case <-pingDone:
				return
			case <-o.wsDone:
				return
			}
		}
	}()
	defer close(pingDone)

	for {
		select {
		case <-o.wsDone:
			return
		default:
		}

		o.wsPrivMu.Lock()
		conn := o.wsPrivConn
		o.wsPrivMu.Unlock()
		if conn == nil {
			time.Sleep(time.Second)
			continue
		}

		// Blocking read with 60s deadline (must be > ping interval of 25s)
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if !isShutdown(o.wsDone) {
				log.Warn().Err(err).Msg("OKX private WS read error, reconnecting...")
				o.reconnectPrivateWS()
			}
			continue
		}

		msgStr := string(msg)
		if msgStr == "pong" {
			continue
		}

		o.handlePrivateWSMessage(msg)
	}
}

// handlePrivateWSMessage parses and dispatches private WS messages.
func (o *OKXExchange) handlePrivateWSMessage(msg []byte) {
	var wsMsg struct {
		Arg  struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wsMsg); err != nil {
		return
	}

	switch wsMsg.Arg.Channel {
	case "orders":
		o.handleOrderUpdate(wsMsg.Data)
	case "account":
		// Account updates — could track balance changes
		log.Debug().RawJSON("data", wsMsg.Data).Msg("Account update received")
	}
}

// handleOrderUpdate parses order fill events and dispatches to handlers.
func (o *OKXExchange) handleOrderUpdate(data json.RawMessage) {
	var orders []struct {
		OrdID   string `json:"ordId"`
		InstID  string `json:"instId"`
		ClOrdID string `json:"clOrdId"`
		Side    string `json:"side"`
		OrdType string `json:"ordType"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		FillSz  string `json:"fillSz"`
		FillPx  string `json:"avgPx"`
		State   string `json:"state"`
		TS      string `json:"uTime"`
	}
	if err := json.Unmarshal(data, &orders); err != nil {
		return
	}

	for _, ord := range orders {
		if ord.State != "filled" {
			continue
		}
		fillSz := parseFloat(ord.FillSz)
		if fillSz == 0 {
			continue
		}

		ts, _ := strconv.ParseInt(ord.TS, 10, 64)
		trade := exchange.Trade{
			ID:        ord.OrdID,
			OrderID:   ord.OrdID,
			Pair:      convertInstID(ord.InstID),
			Side:      exchange.OrderSide(ord.Side),
			Price:     parseFloat(ord.FillPx),
			Amount:    fillSz,
			Timestamp: time.UnixMilli(ts),
		}

		log.Info().
			Str("order_id", ord.OrdID).
			Str("side", ord.Side).
			Float64("price", trade.Price).
			Float64("amount", trade.Amount).
			Msg("Order filled via WS")

		o.dispatchFill(trade)
	}
}

// isShutdown checks if the done channel is closed.
func isShutdown(done chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// reconciliationLoop periodically checks for missed fills via REST.
func (o *OKXExchange) reconciliationLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-o.wsDone:
			return
		case <-ticker.C:
			o.reconcileFills()
		}
	}
}

// reconcileFills queries recent closed orders via REST API and checks for fills we missed.
func (o *OKXExchange) reconcileFills() {
	o.reconcileMu.Lock()
	lastTS := o.lastReconcileTS
	o.reconcileMu.Unlock()

	now := time.Now().Unix()
	if lastTS == 0 {
		// First run — just record the timestamp
		o.reconcileMu.Lock()
		o.lastReconcileTS = now
		o.reconcileMu.Unlock()
		return
	}

	// Query OKX closed orders via REST
	since := strconv.FormatInt((lastTS-60)*1000, 10) // ms, 60s overlap
	nowMS := strconv.FormatInt(now*1000, 10)
	endpoint := fmt.Sprintf("/api/v5/trade/orders-history-archive?instType=SWAP&after=%s&before=%s&limit=20", since, nowMS)
	data, err := o.request("GET", endpoint, nil)
	if err != nil {
		log.Debug().Err(err).Msg("Fill reconciliation: REST query failed")
		o.reconcileMu.Lock()
		o.lastReconcileTS = now
		o.reconcileMu.Unlock()
		return
	}

	var resp struct {
		Code string `json:"code"`
		Data []struct {
			OrdID  string `json:"ordId"`
			InstID string `json:"instId"`
			Side   string `json:"side"`
			Sz     string `json:"sz"`
			State  string `json:"state"`
			AvgPx  string `json:"avgPx"`
			UTime  string `json:"uTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		log.Debug().Err(err).Msg("Fill reconciliation: parse failed")
		o.reconcileMu.Lock()
		o.lastReconcileTS = now
		o.reconcileMu.Unlock()
		return
	}

	for _, ord := range resp.Data {
		if ord.State != "filled" {
			continue
		}
		fillSz := parseFloat(ord.Sz)
		if fillSz == 0 {
			continue
		}

		ts, _ := strconv.ParseInt(ord.UTime, 10, 64)
		trade := exchange.Trade{
			ID:        ord.OrdID,
			OrderID:   ord.OrdID,
			Pair:      convertInstID(ord.InstID),
			Side:      exchange.OrderSide(ord.Side),
			Price:     parseFloat(ord.AvgPx),
			Amount:    fillSz,
			Timestamp: time.UnixMilli(ts),
		}

		log.Info().
			Str("order_id", ord.OrdID).
			Str("side", ord.Side).
			Msg("Reconciliation: detected filled order")

		o.dispatchFill(trade)
	}

	o.reconcileMu.Lock()
	o.lastReconcileTS = now
	o.reconcileMu.Unlock()
}

// reconnectPrivateWS reconnects and re-authenticates the private WebSocket.
func (o *OKXExchange) reconnectPrivateWS() {
	o.wsPrivMu.Lock()
	defer o.wsPrivMu.Unlock()

	for i := 0; i < 5; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURLPrivate, nil)
		if err != nil {
			log.Error().Err(err).Int("attempt", i+1).Msg("Private WS reconnect failed")
			time.Sleep(time.Duration(i+1) * 3 * time.Second)
			continue
		}

		// Login
		ts := WSTimestamp()
		sign := Sign(ts, "GET", "/users/self/verify", "", o.config.SecretKey)
		loginMsg := map[string]interface{}{
			"op": "login",
			"args": []map[string]string{
				{
					"apiKey":     o.config.APIKey,
					"passphrase": o.config.Passphrase,
					"timestamp":  ts,
					"sign":       sign,
				},
			},
		}
		if err := conn.WriteJSON(loginMsg); err != nil {
			conn.Close()
			continue
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			continue
		}
		var lr struct{ Event string `json:"event"` }
		json.Unmarshal(msg, &lr)
		if lr.Event == "error" {
			conn.Close()
			continue
		}

		// Re-subscribe orders
		subMsg := map[string]interface{}{
			"op": "subscribe",
			"args": []map[string]string{
				{"channel": "orders", "instType": "SWAP"},
			},
		}
		if err := conn.WriteJSON(subMsg); err != nil {
			conn.Close()
			continue
		}

		o.wsPrivConn = conn
		log.Info().Msg("Private WS reconnected and authenticated")
		return
	}
	log.Error().Msg("Private WS reconnect exhausted all retries")
}

// Close shuts down the exchange client.
func (o *OKXExchange) Close() error {
	close(o.wsDone)
	if o.wsConn != nil {
		o.wsConn.Close()
	}
	if o.wsPrivConn != nil {
		o.wsPrivConn.Close()
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
