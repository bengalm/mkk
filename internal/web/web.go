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
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.card{background:#1a1d28;border-radius:12px;padding:16px;border:1px solid #2a2d3a}
.card h3{color:#8b8fa3;font-size:11px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.5px}
.card .value{font-size:24px;font-weight:700;color:#fff}
.card .sub{font-size:12px;color:#6b7280;margin-top:2px}
.positive{color:#10b981}.negative{color:#ef4444}.neutral{color:#f59e0b}
table{width:100%;border-collapse:collapse;margin-top:8px}
th,td{padding:6px 8px;text-align:left;border-bottom:1px solid #2a2d3a;font-size:12px}
th{color:#6b7280;font-size:10px;text-transform:uppercase}
td{color:#e1e5eb}
.refresh{color:#6b7280;font-size:12px}
.section{margin-top:16px}
.section-title{color:#8b8fa3;font-size:13px;text-transform:uppercase;margin-bottom:8px;letter-spacing:.5px}
/* Grid visualization */
.grid-viz{position:relative;height:300px;background:#1a1d28;border-radius:12px;border:1px solid #2a2d3a;overflow:hidden}
.grid-line{position:absolute;left:60px;right:12px;height:1px;display:flex;align-items:center}
.grid-line .price{position:absolute;left:-55px;font-size:10px;color:#6b7280;width:50px;text-align:right}
.grid-line .bar{flex:1;height:2px;border-radius:1px}
.grid-line.sell .bar{background:linear-gradient(90deg,#ef444440,#ef4444)}
.grid-line.buy .bar{background:linear-gradient(90deg,#10b981,#10b98140)}
.grid-line.filled .bar{background:#2a2d3a;height:1px}
.price-marker{position:absolute;right:12px;width:8px;height:8px;border-radius:50%;background:#3b82f6;transform:translateY(-50%);z-index:10}
.price-label{position:absolute;right:24px;font-size:11px;color:#3b82f6;transform:translateY(-50%);z-index:10}
.sl-line{position:absolute;left:60px;right:12px;height:0;border-top:2px dashed #ef4444;z-index:5}
.sl-label{position:absolute;left:62px;font-size:10px;color:#ef4444;z-index:5}
</style>
</head>
<body>
<div class="header">
  <h1>⚡ MKK Grid Bot</h1>
  <div><span class="status" id="status">● Running</span> <span class="refresh" id="time"></span></div>
</div>
<div class="grid" id="cards">
  <div class="card"><h3>Pair</h3><div class="value" id="v-pair">-</div></div>
  <div class="card"><h3>Direction</h3><div class="value" id="v-dir">-</div></div>
  <div class="card"><h3>Equity</h3><div class="value" id="v-equity">-</div><div class="sub" id="v-return"></div></div>
  <div class="card"><h3>P&L</h3><div class="value" id="v-pnl">-</div></div>
  <div class="card"><h3>Trades</h3><div class="value" id="v-trades">-</div><div class="sub" id="v-fillrate"></div></div>
  <div class="card"><h3>ATR</h3><div class="value" id="v-atr">-</div></div>
  <div class="card"><h3>Stop Loss</h3><div class="value" id="v-sl">-</div></div>
  <div class="card"><h3>Inventory</h3><div class="value" id="v-inventory">-</div></div>
  <div class="card"><h3>Active Orders</h3><div class="value" id="v-orders">-</div></div>
  <div class="card"><h3>Runtime</h3><div class="value" id="v-runtime">-</div></div>
</div>

<div class="section">
  <div class="section-title">Grid Visualization</div>
  <div class="grid-viz" id="grid-viz"></div>
</div>

<div class="section">
  <div class="section-title">AI Signal</div>
  <div class="card">
    <div style="display:flex;gap:20px;align-items:center">
      <div><span style="color:#8b8fa3;font-size:11px">Direction</span><br><span id="ai-dir" style="font-size:18px;font-weight:700">-</span></div>
      <div><span style="color:#8b8fa3;font-size:11px">Confidence</span><br><span id="ai-conf" style="font-size:18px;font-weight:700">-</span></div>
      <div style="flex:1"><span style="color:#8b8fa3;font-size:11px">Reason</span><br><span id="ai-reason" style="font-size:12px;color:#6b7280">-</span></div>
    </div>
  </div>
</div>

<script>
let prevStats={};
async function refresh(){
  try{
    document.getElementById('time').textContent=new Date().toLocaleTimeString();
    const s=await(await fetch('/api/strategy/stats')).json();
    if(!s||s.error)return;

    // Cards
    document.getElementById('v-pair').textContent=s.pair||'-';
    const dirEl=document.getElementById('v-dir');
    dirEl.textContent=(s.effective_dir||'-').toUpperCase();
    dirEl.className='value '+(s.effective_dir==='short'?'negative':s.effective_dir==='long'?'positive':'neutral');

    const eq=s.equity||0;
    document.getElementById('v-equity').textContent='$'+eq.toFixed(2);
    const ret=s.equity_return||0;
    const retEl=document.getElementById('v-return');
    retEl.textContent=(ret>=0?'+':'')+ret.toFixed(2)+'%';
    retEl.className='sub '+(ret>=0?'positive':'negative');

    const pnl=s.profit||0;
    const pnlEl=document.getElementById('v-pnl');
    pnlEl.textContent=(pnl>=0?'+$':'-$')+Math.abs(pnl).toFixed(2);
    pnlEl.className='value '+(pnl>=0?'positive':'negative');

    document.getElementById('v-trades').textContent=s.trades||0;
    document.getElementById('v-fillrate').textContent=(s.fill_rate_h||0).toFixed(1)+'/h';
    document.getElementById('v-atr').textContent=(s.atr||0).toFixed(2);
    document.getElementById('v-sl').textContent=(s.stop_loss||0).toFixed(2);

    const inv=s.inventory_size||0;
    const invEl=document.getElementById('v-inventory');
    invEl.textContent=(inv>0?'+':'')+inv.toFixed(2);
    invEl.className='value '+(inv>0?'positive':'negative');

    document.getElementById('v-orders').textContent=(s.active_orders||0)+'/'+(s.levels||0);
    const rh=s.runtime_hours||0;
    document.getElementById('v-runtime').textContent=rh<1?(rh*60).toFixed(0)+'m':rh.toFixed(1)+'h';

    // Status
    if(s.stopped){
      document.getElementById('status').textContent='● Stopped';
      document.getElementById('status').style.background='#ef4444';
    }else if(s.paused){
      document.getElementById('status').textContent='● Paused';
      document.getElementById('status').style.background='#f59e0b';
    }

    // AI Signal
    if(s.ai_direction){
      const aiDirEl=document.getElementById('ai-dir');
      aiDirEl.textContent=s.ai_direction.toUpperCase();
      aiDirEl.style.color=s.ai_direction==='short'?'#ef4444':s.ai_direction==='long'?'#10b981':'#f59e0b';
    }
    document.getElementById('ai-conf').textContent=((s.ai_confidence||0)*100).toFixed(0)+'%';

    // Grid visualization
    renderGrid(s);
    prevStats=s;
  }catch(e){console.error(e)}
}

function renderGrid(s){
  const viz=document.getElementById('grid-viz');
  if(!s||!s.low||!s.high){viz.innerHTML='<div style="padding:20px;color:#6b7280;text-align:center">Waiting for grid data...</div>';return}
  const low=s.low,high=s.high,sl=s.stop_loss||0;
  const range=high-low;
  const pad=range*0.1;
  const min=low-pad,max=Math.max(high+pad,sl+pad);
  const totalRange=max-min;
  const h=viz.clientHeight-20;
  const toY=(p)=>(20+h-((p-min)/totalRange*h));

  let html='';
  // Stop loss line
  if(sl>0){
    const sy=toY(sl);
    html+='<div class="sl-line" style="top:'+sy+'px"></div>';
    html+='<div class="sl-label" style="top:'+(sy-14)+'px">SL '+sl.toFixed(2)+'</div>';
  }
  // Current price marker
  const lastPrice=s.last_price||((low+high)/2);
  const py=toY(lastPrice);
  html+='<div class="price-marker" style="top:'+py+'px"></div>';
  html+='<div class="price-label" style="top:'+(py-6)+'px">'+lastPrice.toFixed(2)+'</div>';

  // Grid levels
  const spacing=s.spacing_pct||1;
  const dir=s.effective_dir||'both';
  for(let p=low;p<=high*1.001;p*=(1+spacing/100)){
    const y=toY(p);
    let cls='filled';
    if(p>lastPrice&&dir==='short')cls='sell';
    else if(p<lastPrice&&dir==='long')cls='buy';
    else if(dir==='both')cls=p>=lastPrice?'sell':'buy';
    html+='<div class="grid-line '+cls+'" style="top:'+y+'px"><span class="price">'+p.toFixed(2)+'</span><div class="bar"></div></div>';
  }
  viz.innerHTML=html;
}

refresh();setInterval(refresh,3000);
</script>
</body>
</html>`
