# Orderbook Aggregator — BTC-USD Multi-Exchange

## Goal
Real-time BTC-USD order book aggregator across 4 exchanges (Coinbase, Kraken, Binance, Bitstamp).
No REST polling — native WebSocket only with auto-reconnect.

## Tech Stack
- Backend: Python 3.11, asyncio, websockets
- Cache/Pub-Sub: Redis
- Frontend: Next.js (TypeScript), SSE for real-time streaming
- Deployment: Docker + Docker Compose, target VPS/Cloud
- Reverse proxy: Nginx

## Target Architecture
```
/backend
  /connectors      → one file per exchange (coinbase.py, kraken.py, binance.py, bitstamp.py)
  /core
    aggregator.py  → incremental diff aggregation
    normalizer.py  → cross-exchange format normalization
    metrics.py     → OBI calculation + anomaly detection
    redis_pub.py   → Redis pub/sub publishing
  /api
    rest.py        → GET /api/ob/snapshot endpoint
    sse.py         → SSE stream to frontend
  main.py

/frontend          → Next.js app
  /components
    OrderBook.tsx
    OBIChart.tsx
    AnomalyFeed.tsx

docker-compose.yml
```

## Technical Rules
- Zero REST polling to exchanges — native WebSocket only
- Each connector manages its own heartbeat and exponential backoff reconnect
- Message formats are normalized BEFORE entering the aggregator
- Aggregation is incremental: apply diffs only, never re-fetch full snapshot
- API keys (if needed) via environment variables only (.env)
- All Python files must have full type hints

## Metrics to Implement
- OBI (Order Book Imbalance) rolling over 30 levels, 60-second history
- Spoofing detection: order > 5 BTC removed in < 500ms
- Wall detection
- Sweep detection

## API Endpoints
- GET /api/ob/snapshot → aggregated book + metrics as JSON
- GET /api/ob/stream → real-time SSE stream

## Expected behavior from Claude Code
- Always return complete files, never partial snippets
- Always follow the folder structure above
- Everything Dockerized from the start