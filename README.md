# CLASSUSD Exchange (SQL Learning Project)

CLASSUSD is a classroom trading simulator built for IB Computer Science HL 1.
Students use a web app to trade coins, then analyze the resulting SQLite database with SQL queries for their lab work.

## What this project teaches

- SQL query writing against a real dataset (users, balances, trades, prices, blocks)
- Data relationships and joins in a multi-table schema
- Basic blockchain concepts (linked ledger blocks per coin)
- Portfolio valuation using implied prices from recent trades

## Website overview

The website (`public/index.html`) includes:

- `Dashboard`: balances, portfolio value, recent activity, portfolio breakdown
- `Market`: view/create custom student coins
- `Send`: transfer coins and use staking actions
- `Ledger`: full transaction history
- `Rankings`: CLASSUSD leaderboard and overall portfolio leaderboard
- `Chain`: blockchain explorer per coin
- `CSV` / `DB` downloads in the top nav for analysis

## Key backend features

- Flask + SQLite backend in `server.py`
- Dynamic/implied pricing endpoint: `/api/prices`
- Portfolio ranking endpoint: `/api/portfolio_leaderboard`
- Export endpoints:
  - `/api/export/csv`
  - `/api/export/db`
  - `/api/export/schema`

## Running the project

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start the server:

```bash
python server.py
```

3. Open:

```text
http://localhost:5000
```

## SQL lab report (required)

Students must complete the attached lab report document:

- `g:\My Drive\Comp Sci\HL 1\Blockchain\CLASSUSD_SQL_Lab (1).docx`

Recommended student workflow:

1. Register and trade in the CLASSUSD website.
2. Download the latest `DB` (and optionally `CSV`) from the nav bar.
3. Run SQL queries against `classusd.db` for the report tasks.
4. Complete and submit the `CLASSUSD_SQL_Lab (1).docx` lab report.

## Main files in this folder

- `server.py` - Flask app, API routes, schema, business logic
- `public/index.html` - front-end website
- `classusd.db` - SQLite database (created/updated by server)
- `student/` - student scripts/resources
- `SETUP.md` - teacher setup notes
