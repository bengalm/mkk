package okx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"
)

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

// ISOTimestamp returns current time in ISO format required by OKX.
func ISOTimestamp() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.999Z")
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
