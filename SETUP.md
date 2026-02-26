# CLASSUSD v2 — Teacher Setup Guide
### IB Computer Science · Python + SQLite Edition

---

## What's in this folder

```
classusd_v2/
├── server.py          ← The exchange engine (you run this)
├── requirements.txt   ← Python dependencies (just Flask)
├── classusd.db        ← Created automatically on first run
├── public/
│   └── index.html     ← The student trading interface
└── student/
    ├── queries.py     ← Student SQL query starter (Part B+C)
    └── verify_chain.py ← Student blockchain verifier (Part D)
```

---

## Step 1 — Install Python

Go to **https://python.org** → Downloads → get Python 3.11 or newer.

During install on Windows: ✅ check **"Add Python to PATH"**

Verify: open Terminal/Command Prompt and type:
```
python --version
```
Should show `Python 3.11.x` or similar.

---

## Step 2 — Install Flask

Open Terminal/Command Prompt and run:
```
pip install flask
```

---

## Step 3 — Run the server

**Important:** Run from a local folder, NOT Google Drive.
Copy the `classusd_v2` folder to somewhere like `C:\classusd_v2\`

Then:
```
cd C:\classusd_v2
python server.py
```

You'll see:
```
==================================================
  CLASSUSD Exchange Engine
==================================================
  Local:   http://localhost:5000
  Network: http://192.168.1.XX:5000
  DB file: C:\classusd_v2\classusd.db
==================================================
```

**Keep this window open the whole class.** The server stops when you close it.

---

## Step 4 — Find your IP and share with students

**Windows:** Open Command Prompt → type `ipconfig` → look for "IPv4 Address"
**Mac:** System Settings → Network → your connection → IP address

Students go to: `http://[YOUR IP]:5000`

---

## The database file

`classusd.db` is created automatically in the same folder as `server.py`.

Students can download it two ways:
1. **From the website** — click the ⬇ DB or ⬇ CSV button in the nav bar
2. **Direct link** — `http://[your-ip]:5000/api/export/db`

They copy this file to the same folder as their `queries.py` and `verify_chain.py`.

**To get the schema** (for student reference):
`http://[your-ip]:5000/api/export/schema`

---

## Student workflow

```
1. Register + trade on the class website
2. Download classusd.db (⬇ DB button)
3. Put classusd.db in same folder as queries.py
4. Run: python queries.py       ← writes the 4 IB SQL queries
5. Run: python verify_chain.py  ← implements blockchain verification
```

They should re-download the DB periodically to get the latest trades.

---

## Configuring token rules

Open `server.py` and find these lines near the top:

```python
STARTING_BALANCE = 100.0   # how much each student starts with
BURN_RATE        = 0.00    # set to 0.01 for 1% burn per trade
```

Change `BURN_RATE = 0.01` to enable burning.
Restart the server after any changes.

---

## Reset for a new class period

Delete `classusd.db` and restart the server. Everything starts fresh.

---

## Troubleshooting

**"ModuleNotFoundError: No module named 'flask'"**
→ Run `pip install flask` again

**Students can't connect**
→ Windows: allow Python through Windows Defender Firewall when prompted
→ Mac: click Allow if a firewall popup appears
→ Check you're sharing the right IP address

**"address already in use"**
→ Another copy of the server is running. Close it, or change `port=5000` to `port=5001` at the bottom of server.py

---

## Useful URLs (for the teacher)

| URL | What it shows |
|-----|--------------|
| `/` | Trading interface |
| `/api/leaderboard` | JSON leaderboard |
| `/api/ledger` | All trades |
| `/api/chain` | Full blockchain with validity check |
| `/api/export/csv` | Download trades as CSV |
| `/api/export/db` | Download SQLite database |
| `/api/export/schema` | Show SQL table definitions |
