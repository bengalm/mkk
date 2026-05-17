// +build ignore

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	OKX struct {
		APIKey     string `yaml:"api_key"`
		SecretKey  string `yaml:"secret_key"`
		Passphrase string `yaml:"passphrase"`
	} `yaml:"exchange"`
}

func main() {
	data, _ := os.ReadFile("config.yaml")
	var cfg Config
	yaml.Unmarshal(data, &cfg)

	fmt.Println("API Key:", cfg.OKX.APIKey[:8]+"...")

	// Connect
	conn, _, err := websocket.DefaultDialer.Dial("wss://ws.okx.com:8443/ws/v5/private", nil)
	if err != nil {
		fmt.Println("Dial failed:", err)
		os.Exit(1)
	}
	fmt.Println("Connected to private WS!")

	// Login
	ts := time.Now().UTC().Add(-26 * time.Second).Format("2006-01-02T15:04:05.999Z")
	preHash := ts + "GET" + "/users/self/verify" + ""
	mac := hmac.New(sha256.New, []byte(cfg.OKX.SecretKey))
	mac.Write([]byte(preHash))
	sign := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	loginMsg := map[string]interface{}{
		"op": "login",
		"args": []map[string]string{
			{
				"apiKey":     cfg.OKX.APIKey,
				"passphrase": cfg.OKX.Passphrase,
				"timestamp":  ts,
				"sign":       sign,
			},
		},
	}
	conn.WriteJSON(loginMsg)
	fmt.Println("Login sent, waiting response...")

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		fmt.Println("Read error:", err)
		os.Exit(1)
	}
	fmt.Println("Response:", string(msg))

	var resp struct {
		Event string `json:"event"`
		Msg   string `json:"msg"`
	}
	json.Unmarshal(msg, &resp)
	if resp.Event == "error" {
		fmt.Println("LOGIN FAILED:", resp.Msg)
	} else {
		fmt.Println("LOGIN SUCCESS!")
	}

	conn.Close()
	_ = http.DefaultClient
}
