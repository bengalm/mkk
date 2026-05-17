package okx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// serverTimeOffset is the difference between OKX server time and local time.
// Positive = local clock is ahead of OKX.
var (
	serverTimeOffset   time.Duration
	serverTimeOffsetMu sync.RWMutex
)

// init fetches OKX server time on package load to calibrate clock offset.
func init() {
	go fetchServerTimeOffset()
}

// fetchServerTimeOffset fetches OKX server time and computes offset.
func fetchServerTimeOffset() {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("https://www.okx.com/api/v5/public/time")
	if err != nil {
		log.Warn().Err(err).Msg("Failed to fetch OKX server time for clock sync")
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Data []struct {
			TS string `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil || len(result.Data) == 0 {
		log.Warn().Msg("Failed to parse OKX server time")
		return
	}

	tsMs, _ := strconv.ParseInt(result.Data[0].TS, 10, 64)
	serverTime := time.UnixMilli(tsMs)
	localNow := time.Now().UTC()
	offset := localNow.Sub(serverTime)

	serverTimeOffsetMu.Lock()
	serverTimeOffset = offset
	serverTimeOffsetMu.Unlock()

	log.Info().Dur("offset", offset).Msg("OKX server time sync")
}

// Sign generates OKX API signature.
// OKX V5 requires: HMAC-SHA256(timestamp + method + path + body)
func Sign(timestamp, method, path, body, secretKey string) string {
	preHash := timestamp + method + path + body
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(preHash))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// BuildHeaders constructs OKX authentication headers.
func BuildHeaders(apiKey, passphrase, signature, timestamp string) http.Header {
	return http.Header{
		"OK-ACCESS-KEY":        {apiKey},
		"OK-ACCESS-SIGN":       {signature},
		"OK-ACCESS-TIMESTAMP":  {timestamp},
		"OK-ACCESS-PASSPHRASE": {passphrase},
		"Content-Type":         {"application/json"},
	}
}

// ISOTimestamp returns current time adjusted to OKX server clock in ISO format.
// Used for REST API authentication.
func ISOTimestamp() string {
	serverTimeOffsetMu.RLock()
	offset := serverTimeOffset
	serverTimeOffsetMu.RUnlock()
	adjusted := time.Now().UTC().Add(-offset)
	return adjusted.Format("2006-01-02T15:04:05.999Z")
}

// WSTimestamp returns Unix epoch seconds adjusted to OKX server clock.
// Used for WebSocket login authentication.
func WSTimestamp() string {
	serverTimeOffsetMu.RLock()
	offset := serverTimeOffset
	serverTimeOffsetMu.RUnlock()
	adjusted := time.Now().UTC().Add(-offset)
	return fmt.Sprintf("%d", adjusted.Unix())
}

// SignRequest is a convenience function to sign an OKX API request.
func SignRequest(apiKey, secretKey, passphrase, method, path, body string) http.Header {
	timestamp := ISOTimestamp()
	signature := Sign(timestamp, method, path, body, secretKey)
	headers := BuildHeaders(apiKey, passphrase, signature, timestamp)
	// Add simulated trading header for testnet
	headers.Set("x-simulated-trading", "1")
	return headers
}

// ValidateAPIKey checks if API credentials look valid.
func ValidateAPIKey(apiKey, secretKey, passphrase string) error {
	if apiKey == "" {
		return fmt.Errorf("API key is empty")
	}
	if secretKey == "" {
		return fmt.Errorf("secret key is empty")
	}
	if passphrase == "" {
		return fmt.Errorf("passphrase is empty")
	}
	if len(apiKey) < 10 {
		return fmt.Errorf("API key seems too short")
	}
	return nil
}
