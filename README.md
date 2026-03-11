# Uniswap Observatory

Uniswap Observatory is a lightweight local monitoring tool for Uniswap liquidity pools.

It tracks pool health, liquidity depth signals, price movement, and risk indicators in real time using a local Node.js engine and dashboard viewer.

## Features

- Live monitoring of multiple Uniswap pools
- Pool risk scoring system
- Event detection (STATE_STALE, QUIET_POOL, etc)
- Sparkline price movement charts
- Liquidity depth risk indicators
- Market context display (ETH / BTC prices)
- Alert banner for pool risk events
- Local dashboard viewer

## Architecture

The system has two main components:

Engine
- Node.js monitoring engine
- Polls Uniswap pools
- Generates structured snapshots
- Writes `latest.json`

Viewer
- Local dashboard interface
- Reads `latest.json`
- Displays pool health and risk signals

## Project Structure

