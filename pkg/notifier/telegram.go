package notifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bengalm/mkk/pkg/config"
	"github.com/rs/zerolog/log"
)

// Event types used for notification routing.
const (
	EventPositionOpened = "position_opened"
	EventPositionClosed = "position_closed"
	EventRiskTriggered  = "risk_triggered"
	EventError          = "error"
)

// TelegramNotifier sends trade notifications via the Telegram Bot API.
type TelegramNotifier struct {
	cfg    config.NotifierConfig
	client *http.Client
}

// New creates a new TelegramNotifier from botToken and chatID.
func New(botToken, chatID string) *TelegramNotifier {
	return &TelegramNotifier{
		cfg: config.NotifierConfig{
			BotToken: strings.TrimSpace(botToken),
			ChatID:   strings.TrimSpace(chatID),
		},
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// NewTelegramNotifier creates a new notifier from config.
func NewTelegramNotifier(cfg config.NotifierConfig) *TelegramNotifier {
	return &TelegramNotifier{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Send sends a plain text message via Telegram (convenience method).
func (n *TelegramNotifier) Send(message string) error {
	return n.send(message)
}

// NotifyTrade implements trader.Notifier interface.
func (n *TelegramNotifier) NotifyTrade(pair, side string, price, amount float64) error {
	return n.send(fmt.Sprintf("📊 <b>Trade Executed</b>\nPair: <code>%s</code>\nSide: %s\nPrice: <code>%.2f</code>\nAmount: <code>%.6f</code>", pair, strings.ToUpper(side), price, amount))
}

// NotifyPnL implements trader.Notifier interface.
func (n *TelegramNotifier) NotifyPnL(pair string, pnl float64) error {
	emoji := "📈"
	if pnl < 0 {
		emoji = "📉"
	}
	return n.send(fmt.Sprintf("%s <b>Position Closed</b>\nPair: <code>%s</code>\nP&L: <code>%.2f USDT</code>", emoji, pair, pnl))
}

// NotifyError implements trader.Notifier interface.
func (n *TelegramNotifier) NotifyError(context string, errMsg string) error {
	return n.send(fmt.Sprintf("🚨 <b>Error</b>\nContext: %s\nError: <code>%s</code>", context, errMsg))
}

// NotifyRiskAlert implements trader.Notifier interface.
func (n *TelegramNotifier) NotifyRiskAlert(message string) error {
	return n.send(fmt.Sprintf("⚠️ <b>Risk Alert</b>\n%s", message))
}

// Notify sends a formatted Telegram message for the given event.
func (n *TelegramNotifier) Notify(eventType string, data map[string]interface{}) {
	if n.cfg.BotToken == "" || n.cfg.ChatID == "" {
		log.Debug().Msg("Telegram notifier not configured, skipping notification")
		return
	}

	text := n.formatMessage(eventType, data)
	if text == "" {
		log.Warn().Str("event_type", eventType).Msg("Unknown event type, skipping notification")
		return
	}

	if err := n.send(text); err != nil {
		log.Error().Err(err).Str("event_type", eventType).Msg("Failed to send Telegram notification")
	}
}

// formatMessage builds a concise emoji-prefixed message for the event.
func (n *TelegramNotifier) formatMessage(eventType string, data map[string]interface{}) string {
	switch eventType {
	case EventPositionOpened:
		return n.formatPositionOpened(data)
	case EventPositionClosed:
		return n.formatPositionClosed(data)
	case EventRiskTriggered:
		return n.formatRiskTriggered(data)
	case EventError:
		return n.formatError(data)
	default:
		return ""
	}
}

func (n *TelegramNotifier) formatPositionOpened(d map[string]interface{}) string {
	pair := strVal(d, "pair")
	side := strVal(d, "side")
	price := floatVal(d, "price")
	amount := floatVal(d, "amount")
	strategy := strVal(d, "strategy")

	var sb strings.Builder
	sb.WriteString("🟢 <b>Position Opened</b>\n")
	sb.WriteString(fmt.Sprintf("Pair: <code>%s</code>\n", pair))
	sb.WriteString(fmt.Sprintf("Side: %s\n", strings.ToUpper(side)))
	sb.WriteString(fmt.Sprintf("Price: <code>%.4f</code>\n", price))
	sb.WriteString(fmt.Sprintf("Amount: <code>%.6f</code>\n", amount))
	if strategy != "" {
		sb.WriteString(fmt.Sprintf("Strategy: %s\n", strategy))
	}
	if sl, ok := floatValOk(d, "stop_loss"); ok && sl > 0 {
		sb.WriteString(fmt.Sprintf("Stop Loss: <code>%.4f</code>\n", sl))
	}
	if tp, ok := floatValOk(d, "take_profit"); ok && tp > 0 {
		sb.WriteString(fmt.Sprintf("Take Profit: <code>%.4f</code>\n", tp))
	}
	return sb.String()
}

func (n *TelegramNotifier) formatPositionClosed(d map[string]interface{}) string {
	pair := strVal(d, "pair")
	pnl := floatVal(d, "pnl")
	price := floatVal(d, "price")
	dailyPnL := floatVal(d, "daily_pnl")

	pnlEmoji := "📊"
	if pnl > 0 {
		pnlEmoji = "💰"
	} else if pnl < 0 {
		pnlEmoji = "🔻"
	}

	var sb strings.Builder
	sb.WriteString("🔴 <b>Position Closed</b>\n")
	sb.WriteString(fmt.Sprintf("Pair: <code>%s</code>\n", pair))
	sb.WriteString(fmt.Sprintf("Price: <code>%.4f</code>\n", price))
	sb.WriteString(fmt.Sprintf("%s PnL: <code>%.4f</code>\n", pnlEmoji, pnl))
	if dailyPnL != 0 {
		sb.WriteString(fmt.Sprintf("Daily PnL: <code>%.4f</code>\n", dailyPnL))
	}
	return sb.String()
}

func (n *TelegramNotifier) formatRiskTriggered(d map[string]interface{}) string {
	reason := strVal(d, "reason")
	drawdown := floatVal(d, "drawdown")
	maxDrawdown := floatVal(d, "max_drawdown")

	var sb strings.Builder
	sb.WriteString("⚠️ <b>Risk Alert</b>\n")
	if reason != "" {
		sb.WriteString(fmt.Sprintf("Reason: %s\n", reason))
	}
	if drawdown > 0 {
		sb.WriteString(fmt.Sprintf("Drawdown: <code>%.2f%%</code>\n", drawdown))
	}
	if maxDrawdown > 0 {
		sb.WriteString(fmt.Sprintf("Max Drawdown: <code>%.2f%%</code>\n", maxDrawdown))
	}
	sb.WriteString("Engine stopped.")
	return sb.String()
}

func (n *TelegramNotifier) formatError(d map[string]interface{}) string {
	errMsg := strVal(d, "error")
	pair := strVal(d, "pair")

	var sb strings.Builder
	sb.WriteString("❌ <b>Error</b>\n")
	if pair != "" {
		sb.WriteString(fmt.Sprintf("Pair: <code>%s</code>\n", pair))
	}
	if errMsg != "" {
		// Truncate long error messages
		if len(errMsg) > 500 {
			errMsg = errMsg[:497] + "..."
		}
		sb.WriteString(fmt.Sprintf("Message: <code>%s</code>\n", errMsg))
	}
	return sb.String()
}

// send posts a message to the Telegram Bot API.
func (n *TelegramNotifier) send(text string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", n.cfg.BotToken)

	payload := map[string]interface{}{
		"chat_id":    n.cfg.ChatID,
		"text":       text,
		"parse_mode": "HTML",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal telegram payload: %w", err)
	}

	resp, err := n.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("telegram api request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram api returned status %d: %s", resp.StatusCode, string(respBody))
	}

	log.Debug().Str("chat_id", n.cfg.ChatID).Msg("Telegram notification sent")
	return nil
}

// strVal extracts a string value from a map, returning "" if missing.
func strVal(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprintf("%v", v)
	}
	return s
}

// floatVal extracts a float64 value from a map, returning 0 if missing.
func floatVal(m map[string]interface{}, key string) float64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

// floatValOk extracts a float64 value from a map, with an ok flag.
func floatValOk(m map[string]interface{}, key string) (float64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
