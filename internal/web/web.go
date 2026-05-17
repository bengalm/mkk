package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/bengalm/mkk/pkg/paper"
	"github.com/bengalm/mkk/pkg/repository"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/bengalm/mkk/pkg/trader"
	"github.com/rs/zerolog/log"
)

// Server provides the web API and dashboard.
type Server struct {
	port     int
	repo     *repository.Repository
	trader   *trader.Engine
	paper    *paper.PaperEngine
	strategy strategy.Strategy
	mux      *http.ServeMux
}

// SetStrategy sets the active strategy for stats endpoint.
func (s *Server) SetStrategy(st strategy.Strategy) { s.strategy = st }

// NewServer creates a new web server.
func NewServer(port int, repo *repository.Repository) *Server {
	s := &Server{
		port: port,
		repo: repo,
		mux:  http.NewServeMux(),
	}
	s.routes()
	return s
}

// SetTrader sets the live trading engine.
func (s *Server) SetTrader(t *trader.Engine) { s.trader = t }

// SetPaper sets the paper trading engine.
func (s *Server) SetPaper(p *paper.PaperEngine) { s.paper = p }

// Start starts the HTTP server.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Info().Str("addr", addr).Msg("Web server starting")
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) routes() {
	// API endpoints
	s.mux.HandleFunc("/api/status", s.handleStatus)
	s.mux.HandleFunc("/api/positions", s.handlePositions)
	s.mux.HandleFunc("/api/trades", s.handleTrades)
	s.mux.HandleFunc("/api/pnl", s.handlePnL)
	s.mux.HandleFunc("/api/balance", s.handleBalance)
	s.mux.HandleFunc("/api/strategies", s.handleStrategies)
	s.mux.HandleFunc("/api/candles", s.handleCandles)
	s.mux.HandleFunc("/api/strategy/stats", s.handleStrategyStats)

	// Dashboard
	s.mux.HandleFunc("/", s.handleDashboard)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":    "running",
		"timestamp": time.Now().Format(time.RFC3339),
	}
	if s.trader != nil {
		status["trader"] = s.trader.GetStats()
	}
	if s.paper != nil {
		status["paper"] = s.paper.Summary()
	}
	s.jsonResponse(w, status)
}

func (s *Server) handlePositions(w http.ResponseWriter, r *http.Request) {
	if s.trader != nil {
		s.jsonResponse(w, s.trader.GetPositions())
		return
	}
	if s.paper != nil {
		s.jsonResponse(w, s.paper.GetPositions())
		return
	}

	// Fallback to DB
	positions, err := s.repo.GetOpenPositions()
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.jsonResponse(w, positions)
}

func (s *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	trades, err := s.repo.GetTradeHistory(100)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.jsonResponse(w, trades)
}

func (s *Server) handlePnL(w http.ResponseWriter, r *http.Request) {
	summary, err := s.repo.GetPnLSummary(30)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.jsonResponse(w, summary)
}

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	if s.paper != nil {
		s.jsonResponse(w, map[string]interface{}{
			"balance": s.paper.GetBalance(),
			"equity":  s.paper.GetEquity(),
		})
		return
	}
	s.errorResponse(w, http.StatusNotImplemented, "no paper engine")
}

func (s *Server) handleStrategies(w http.ResponseWriter, r *http.Request) {
	s.jsonResponse(w, map[string]interface{}{
		"strategies": []string{"grid", "dca", "rsi"},
	})
}

func (s *Server) handleStrategyStats(w http.ResponseWriter, r *http.Request) {
	if s.strategy == nil {
		s.errorResponse(w, http.StatusNotFound, "no active strategy")
		return
	}
	s.jsonResponse(w, s.strategy.Stats())
}

func (s *Server) handleCandles(w http.ResponseWriter, r *http.Request) {
	pair := r.URL.Query().Get("pair")
	timeframe := r.URL.Query().Get("timeframe")
	if pair == "" {
		pair = "BTC-USDT"
	}
	if timeframe == "" {
		timeframe = "1h"
	}

	candles, err := s.repo.GetCandles(pair, timeframe, time.Now().Add(-72*time.Hour), time.Now(), 500)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.jsonResponse(w, candles)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, dashboardHTML)
}

func (s *Server) jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *Server) errorResponse(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MKK Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e1e5eb;padding:20px}
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}
.header h1{font-size:24px;color:#fff}
.header .status{padding:4px 12px;border-radius:12px;font-size:12px;background:#10b981;color:#fff}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
.card{background:#1a1d28;border-radius:12px;padding:20px;border:1px solid #2a2d3a}
.card h3{color:#8b8fa3;font-size:12px;text-transform:uppercase;margin-bottom:12px}
.card .value{font-size:28px;font-weight:700;color:#fff}
.card .label{font-size:13px;color:#6b7280;margin-top:4px}
.positive{color:#10b981}.negative{color:#ef4444}
table{width:100%;border-collapse:collapse;margin-top:12px}
th,td{padding:8px;text-align:left;border-bottom:1px solid #2a2d3a}
th{color:#6b7280;font-size:11px;text-transform:uppercase}
td{color:#e1e5eb;font-size:13px}
.refresh{color:#6b7280;font-size:12px}
</style>
</head>
<body>
<div class="header">
  <h1>🤖 MKK Dashboard</h1>
  <div><span class="status" id="status">● Running</span> <span class="refresh" id="time"></span></div>
</div>
<div class="grid">
  <div class="card"><h3>Balance</h3><div class="value" id="balance">-</div></div>
  <div class="card"><h3>Equity</h3><div class="value" id="equity">-</div></div>
  <div class="card"><h3>Daily P&L</h3><div class="value" id="pnl">-</div></div>
  <div class="card"><h3>Open Positions</h3><div class="value" id="positions-count">-</div></div>
</div>
<div class="card" style="margin-top:16px">
  <h3>Recent Trades</h3>
  <table><thead><tr><th>Time</th><th>Pair</th><th>Side</th><th>Price</th><th>P&L</th></tr></thead>
  <tbody id="trades"></tbody></table>
</div>
<script>
async function refresh(){
  try{
    const s=await(await fetch('/api/status')).json();
    document.getElementById('time').textContent=new Date().toLocaleTimeString();
    if(s.paper){
      document.getElementById('balance').textContent='$'+s.paper.current_balance;
      document.getElementById('equity').textContent='$'+s.paper.equity;
      document.getElementById('positions-count').textContent=s.paper.open_positions;
      const pnl=s.paper.total_pnl;
      const el=document.getElementById('pnl');
      el.textContent=(pnl>=0?'+':'')+pnl.toFixed(2);
      el.className='value '+(pnl>=0?'positive':'negative');
    }
    const t=await(await fetch('/api/trades')).json();
    const tb=document.getElementById('trades');
    tb.innerHTML=(t||[]).slice(0,10).map(x=>'<tr><td>'+x.timestamp+'</td><td>'+x.pair+'</td><td>'+x.side+'</td><td>'+x.price+'</td><td class="'+(x.pnl>=0?'positive':'negative')+'">'+x.pnl+'</td></tr>').join('');
  }catch(e){console.error(e)}
}
refresh();setInterval(refresh,5000);
</script>
</body>
</html>`
