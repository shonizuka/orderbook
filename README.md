# BTC-USD Order Book Aggregator

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Next.js](https://img.shields.io/badge/Next.js-14-black)
![Redis](https://img.shields.io/badge/Redis-7-red)
![Railway](https://img.shields.io/badge/Deployed-Railway-blueviolet)
[![Live Demo](https://img.shields.io/badge/Live-Demo-green)](https://frontend-production-a0e3.up.railway.app)

Real-time BTC-USD order book aggregator across 4 exchanges — Coinbase, Kraken, Binance, and Bitstamp. Built for market microstructure analysis with live anomaly detection.

Live demo: https://frontend-production-a0e3.up.railway.app/
---

## What it does

- Connects to all 4 exchanges simultaneously via native WebSocket — no REST polling
- Merges order books in real-time into a single aggregated view
- Calculates OBI (Order Book Imbalance) with 60-second rolling history across 30 price levels
- Detects market anomalies automatically: spoofing, large walls, and sweep events
- Streams everything live to the browser via SSE (Server-Sent Events)

## Architecture


Each exchange has its own connector with auto-reconnect and heartbeat. Messages are normalized into a common format before entering the aggregator.

## Stack

| Layer | Tech |
|-------|------|
| Backend | Python 3.11, asyncio, FastAPI |
| Connectors | Native WebSockets (websockets lib) |
| Cache / Pub-Sub | Redis 7 |
| Frontend | Next.js 14, TypeScript, Tailwind, Recharts |
| Infra | Docker Compose, Railway |

## Run locally

**Requirements:** Docker + Docker Compose

```bash
git clone https://github.com/shonizuka/orderbook.git
cd orderbook
docker compose up --build
