# Oracle DBA Intelligence

> AI-Powered Oracle Database Performance Analysis Platform — Production-Ready SaaS

🔗 **Live Demo:** [https://db-guardian-ai.vercel.app/](https://db-guardian-ai.vercel.app/)

A modern, enterprise-grade SaaS platform that performs AI-driven Root Cause Analysis (RCA) on Oracle Database workloads. Upload AWR/ASH reports and get instant, expert-level DBA insights with actionable recommendations.

---

## Features

- **AI DBA Expert Engine** — Analyzes like a Senior Oracle DBA, identifies only truly problematic SQL (1-3 max)
- **AWR + ASH Analysis** — Parses Oracle AWR and ASH HTML reports into structured data
- **5-Point Deep Analysis** — Problem summary, technical params, execution patterns, DBA interpretation, wait event linkage
- **Actionable Recommendations** — Specific tuning commands, index suggestions, SQL rewrites
- **Per-User Data Isolation** — Complete multi-tenant data separation
- **Modern Dark UI** — SaaS-grade responsive interface with glassmorphism design
- **Vercel Deployable** — Production-ready with deployment config included

---

## Quick Start

```bash
# Clone & install
git clone https://github.com/siddhesh940/DBGuardian-AI.git
cd Final_Workload_Project
pip install -r requirements.txt

# Run
python app.py
```

Open `http://localhost:4539` → Register → Upload AWR/ASH reports → Run Analysis

---

## Tech Stack

| Layer      | Technology                                        |
| ---------- | ------------------------------------------------- |
| Backend    | Python, FastAPI, Uvicorn                          |
| Frontend   | HTML5, CSS3 (Custom Design System), Vanilla JS    |
| Parsing    | BeautifulSoup4, lxml, Pandas                      |
| AI Engine  | Custom DBA Expert Engine (Rule-based + Heuristic) |
| Deployment | Vercel / Any ASGI host                            |

---

## Project Structure

```
├── app.py                        # FastAPI application entry point
├── vercel.json                   # Vercel deployment config
├── requirements.txt              # Python dependencies
│
├── api/
│   ├── auth_routes.py            # Authentication (register/login/logout)
│   └── rca_routes.py             # Upload, RCA analysis, results API
│
├── engine/
│   ├── rca_engine.py             # Main RCA orchestrator
│   ├── dba_expert_engine.py      # AI DBA analysis engine
│   ├── dba_formatter.py          # Output formatting (API/Console/Summary)
│   ├── awr_analyzer.py           # AWR data analysis
│   ├── ash_analyzer.py           # ASH data analysis
│   ├── time_window_detector.py   # Auto time period detection
│   ├── decision_engine.py        # Decision logic
│   └── unified_metrics.py        # Metrics utilities
│
├── parsers/
│   ├── awr_html_parser.py        # AWR HTML → CSV
│   ├── ash_html_parser.py        # ASH HTML → CSV
│   └── snapshot_metadata_parser.py
│
├── agent/
│   └── sql_agent.py              # SQL fix recommendation agent
│
├── ui/
│   ├── static/                   # CSS Design System + JavaScript
│   └── templates/                # HTML templates (Login, Dashboard, Results)
│
└── data/
    ├── users.json                # User credentials
    └── users/<username>/         # Per-user isolated storage
```

---

## API Reference

### Auth

| Method | Endpoint         | Description        |
| ------ | ---------------- | ------------------ |
| `POST` | `/auth/register` | Create new account |
| `POST` | `/auth/login`    | Sign in            |
| `POST` | `/auth/logout`   | Sign out           |

### Analysis

| Method | Endpoint       | Description                   |
| ------ | -------------- | ----------------------------- |
| `POST` | `/api/upload`  | Upload AWR/ASH HTML files     |
| `POST` | `/api/run_rca` | Run AI-powered RCA analysis   |
| `GET`  | `/api/results` | Fetch latest analysis results |

### Pages

| Route         | Page                              |
| ------------- | --------------------------------- |
| `/`           | Login                             |
| `/register`   | Registration                      |
| `/dashboard`  | Main dashboard (upload + analyze) |
| `/results`    | View analysis results             |
| `/newresults` | AI expert analysis view           |

---

## DBA Expert Analysis

The AI engine uses strict thresholds to identify problematic SQL:

| Criterion    | HIGH    | CRITICAL |
| ------------ | ------- | -------- |
| Elapsed Time | > 50s   | > 100s   |
| CPU Time     | > 20s   | > 50s    |
| Executions   | > 1,000 | > 5,000  |
| Workload %   | > 10%   | > 20%    |
| Avg/Exec     | > 0.5s  | > 2.0s   |

### Analysis Output (per SQL)

1. **Problem Summary** — Why it's problematic + database impact
2. **Technical Parameters** — All key performance metrics
3. **Execution Pattern** — High frequency / Bursty / Sustained
4. **DBA Interpretation** — Root cause (CPU-bound, missing index, bad plan, etc.)
5. **Wait Event Linkage** — Correlation with CPU/IO/Latch waits

---

## Deployment

### Vercel (Recommended)

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

`vercel.json` is pre-configured.

### Docker (Alternative)

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Any ASGI Host

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

---

## Environment

- Python 3.9+
- No external database required (file-based storage)
- No API keys needed

---

## License

MIT

---

Built with FastAPI + AI-Powered DBA Intelligence Engine
