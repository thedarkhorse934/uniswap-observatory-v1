# 🛰️ Uniswap Observatory

A **local monitoring tool for Uniswap liquidity pools** that detects price movement, liquidity depth changes, and risk signals in real time.

The project combines a **Node.js monitoring engine** with a **local dashboard viewer** to observe how liquidity pools behave and surface useful signals about pool health and market activity.

---

# 📸 Dashboard

![Dashboard](screenshots/observatory-dashboard.png)

The dashboard displays:

• Pool prices  
• Liquidity depth risk  
• Event alerts  
• Sparkline price movement  
• Pool health scoring  
• Market context (ETH & BTC prices)

---

# 🎯 Project Goals

This project was built to explore:

• **Uniswap pool mechanics**  
• **AMM liquidity behaviour**  
• **Depth / slippage risk signals**  
• **Event-driven blockchain monitoring**  
• **Building real-time dashboards from on-chain data**

The aim was not to build a trading bot, but to create a **monitoring tool that helps understand liquidity conditions and pool risk in real time.**

---

# 🧠 Key Concepts

Uniswap Observatory monitors several indicators:

### Pool Price
Current pool price derived from the pool reserves.

### Liquidity Depth
How much price impact a trade would cause.

Example signal: Depth fragile (~5.96% impact for $100k)


This indicates the pool may experience **high slippage under large trades.**

### Pool Risk Score
Each pool receives a **risk score** based on:

• liquidity depth  
• recent pool activity  
• event signals  

Scores help quickly identify pools under stress.

### Events
The engine detects events such as:

| Event | Meaning |
|------|------|
STATE_STALE | Pool data hasn't updated recently |
QUIET_POOL | No recent swaps |
FLOW_SURGE | Large liquidity movement |
DEPTH_FRAGMENT | Liquidity becoming fragmented |
LIQUIDITY_EXIT | Liquidity leaving pool |
MEV_SWARM | Increased MEV / arbitrage activity |

These are surfaced in the dashboard as **event badges**.

---

# ⚙️ Architecture

The system has two main components.

       Ethereum RPC
            │
            ▼
    Node.js Monitoring Engine
            │
     Generates snapshots
            │
     latest.json output
            │
            ▼
     Local Dashboard Viewer
       (Electron / Browser)

---

## 📁 Project Structure

```
uniswap-observatory-v1
│
├── index.js
│   Node.js monitoring engine
│
├── main.cjs
│   Electron desktop launcher
│
├── package.json
│   Project dependencies and scripts
│
├── package-lock.json
│   Dependency lockfile
│
├── pools.json
│   Pool configuration used by the monitoring engine
│
├── pools.template.json
│   Example configuration template
│
├── viewer
│   ├── index.html
│   │   Dashboard UI layout
│   │
│   ├── style.css
│   │   Dashboard styling
│   │
│   └── app.js
│       Dashboard logic and data rendering
│
├── README.md
│   Project documentation
│
├── LICENSE
│   MIT license
│
└── .gitignore
    Files excluded from the repository
```


---

# 🚀 Features

### 📊 Pool Monitoring
Tracks multiple Uniswap pools simultaneously.

### ⚠️ Risk Scoring
Each pool receives a score indicating relative risk.

### 📉 Liquidity Depth Analysis
Detects when pools become fragile or thin.

### 🔔 Event Detection
Flags conditions such as stale pools, liquidity exits, or unusual activity.

### 📈 Price Movement Sparklines
Displays recent price movement directly in the dashboard.

### 🌍 Market Context
Displays:

• ETH price  
• BTC price (derived from WBTC/ETH)  
• Stablecoin peg health

### 🖥️ Desktop Viewer
Dashboard can run locally using Electron.

---

# 🛠️ Installation

Clone the repository: git clone https://github.com/thedarkhorse934/uniswap-observatory-v1.git

Enter the project directory: 

```
cd uniswap-observatory-v1
```

Install dependencies: 

```
npm install
```
---

# ▶️ Running the Observatory

### Start the monitoring engine

```
node index.js
```

This will begin polling pools and generating:
```
latest.json
```
---

### Launch the dashboard viewer

```
npm run desktop
```

This opens the **local dashboard viewer**.

The dashboard automatically refreshes every few seconds.

---

# 📊 Example Output

Example signals detected by the system: 


**⚠ 1 pool stale**

**Depth fragile (~5.96% impact for $100k)**

**Flow surge detected**


These signals help identify **potential liquidity risk conditions.**

---

# 🔍 Pools Monitored

Example pools included in the configuration:

- USDC / USDT
- USDC / WETH
- USDT / WETH
- WBTC / WETH


These pools allow the system to derive:

• ETH/USD  
• BTC/USD  
• Stablecoin peg stability

---

# 📚 What This Project Explores

This project touches several DeFi mechanics:

### Automated Market Makers (AMMs)

How pools determine price based on token reserves.

### Liquidity Depth

How large trades affect price impact.

### Slippage

Why thin liquidity pools can cause large price movements.

### MEV Activity

How arbitrage bots interact with pools.

### Event-Driven Monitoring

Detecting unusual conditions in blockchain systems.

---

# 🔮 Future Ideas

Possible future improvements include:

• Cross-DEX monitoring  
• Whale swap detection  
• Liquidity flow tracking  
• Multi-chain monitoring  
• Predictive pool risk signals  
• Automated alerting system

---

# 🎓 Learning Outcomes

Building this project involved:

• Node.js development  
• Blockchain RPC interaction  
• DeFi protocol mechanics  
• Event detection systems  
• Dashboard UI development  
• Git / GitHub workflow

---

# 🪪 License

MIT License

This project is open-source and free to use or modify.

---

# 👤 Author

GitHub  
https://github.com/thedarkhorse934

---

# ⭐ If you found this interesting

Feel free to star the repo or fork it to experiment further with DeFi monitoring tools.











