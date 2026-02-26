"""
CLASSUSD Exchange Engine v3
IB Computer Science Blockchain Project

Features:
  - Students create their own coins with custom rules
  - Burn, Airdrop, Anti-whale, Staking token mechanics
  - Full blockchain ledger per coin
  - SQL-queryable SQLite database
  - CSV + DB export for student analysis
"""

import sqlite3
import hashlib
import csv
import io
import os
import json
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_file, send_from_directory, g

app = Flask(__name__, static_folder='public')

DB_PATH          = 'classusd.db'
TEACHER_KEY = os.environ.get('CLASSUSD_TEACHER_KEY','')
RESERVE_CURRENCY = 'CLASSUSD'   # the base currency everyone starts with
STARTING_BALANCE = 100.0        # how much CLASSUSD each student gets on signup
STAKING_RATE     = 0.02         # 2% per manual claim (teacher can adjust)


# ─────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db: db.close()

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        -- ── USERS ──────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS users (
            user_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            handle        TEXT    NOT NULL UNIQUE COLLATE NOCASE,
            password_hash TEXT    NOT NULL,
            created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        -- ── COINS ──────────────────────────────────────────────────
        -- Each student creates one coin. CLASSUSD is the reserve coin.
        CREATE TABLE IF NOT EXISTS coins (
            coin_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol        TEXT    NOT NULL UNIQUE,
            name          TEXT    NOT NULL,
            creator_id    INTEGER REFERENCES users(user_id),
            total_supply  REAL    NOT NULL DEFAULT 1000.0,
            burn_rate     REAL    NOT NULL DEFAULT 0.0
                CHECK(burn_rate >= 0 AND burn_rate <= 0.5),
            airdrop_amount REAL   NOT NULL DEFAULT 0.0,
            max_holding   REAL,         -- NULL = no anti-whale limit
            staking_enabled INTEGER NOT NULL DEFAULT 0,
            description   TEXT,
            created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        -- ── BALANCES ───────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS balances (
            balance_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES users(user_id),
            coin_id     INTEGER NOT NULL REFERENCES coins(coin_id),
            amount      REAL    NOT NULL DEFAULT 0.0 CHECK(amount >= 0),
            staked      REAL    NOT NULL DEFAULT 0.0 CHECK(staked >= 0),
            last_staked TEXT,
            UNIQUE(user_id, coin_id)
        );

        -- ── TRADES ─────────────────────────────────────────────────
        -- Every movement of any coin (send, airdrop, stake reward, etc.)
        CREATE TABLE IF NOT EXISTS trades (
            trade_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id  INTEGER REFERENCES users(user_id),  -- NULL = system/BANK
            to_user_id    INTEGER NOT NULL REFERENCES users(user_id),
            coin_id       INTEGER NOT NULL REFERENCES coins(coin_id),
            amount        REAL    NOT NULL CHECK(amount > 0),
            burned_amount REAL    NOT NULL DEFAULT 0.0,
            trade_type    TEXT    NOT NULL DEFAULT 'transfer',
                -- values: 'transfer', 'airdrop', 'stake_reward', 'welcome'
            note          TEXT,
            executed_at   TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        -- ── BLOCKCHAIN ─────────────────────────────────────────────
        -- One chain per coin — each coin has its own linked block history
        CREATE TABLE IF NOT EXISTS ledger_blocks (
            block_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id     INTEGER NOT NULL REFERENCES coins(coin_id),
            trade_id    INTEGER NOT NULL REFERENCES trades(trade_id),
            prev_hash   TEXT    NOT NULL,
            block_data  TEXT    NOT NULL,
            this_hash   TEXT    NOT NULL,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        
        -- ── PRICES (for tracking portfolio value in CLASSUSD) ───────────────
        -- Teacher can set a reference price for each coin in CLASSUSD.
        CREATE TABLE IF NOT EXISTS prices (
            coin_id     INTEGER PRIMARY KEY REFERENCES coins(coin_id),
            price       REAL    NOT NULL DEFAULT 1.0,   -- in CLASSUSD per 1 coin
            updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
        );

        -- ── VIEWS ──────────────────────────────────────────────────
        DROP VIEW IF EXISTS trade_history;
        CREATE VIEW trade_history AS
            SELECT
                t.trade_id,
                COALESCE(s.handle, 'SYSTEM') AS from_user,
                r.handle                      AS to_user,
                c.symbol,
                c.name                        AS coin_name,
                t.amount,
                t.burned_amount,
                t.trade_type,
                t.note,
                t.executed_at,
                lb.this_hash                  AS block_hash
            FROM trades t
            LEFT JOIN users s  ON t.from_user_id = s.user_id
            JOIN  users r      ON t.to_user_id   = r.user_id
            JOIN  coins c      ON t.coin_id       = c.coin_id
            LEFT JOIN ledger_blocks lb ON lb.trade_id = t.trade_id;

        DROP VIEW IF EXISTS leaderboard;
        CREATE VIEW leaderboard AS
            SELECT
                u.handle,
                c.symbol,
                c.name  AS coin_name,
                b.amount,
                b.staked
            FROM balances b
            JOIN users u ON b.user_id = u.user_id
            JOIN coins c ON b.coin_id = c.coin_id
            ORDER BY c.symbol, b.amount DESC;
    """)

    # Seed the reserve currency
    conn.execute("""
        INSERT OR IGNORE INTO coins
            (symbol, name, creator_id, total_supply, burn_rate, description)
        VALUES (?, ?, NULL, 999999, 0.0, 'The class reserve currency. Everyone starts with 100.')
    """, (RESERVE_CURRENCY, 'ClassUSD'))
    conn.commit()

    # Seed price rows for any coins that don't have one yet
    conn.execute("""
        INSERT OR IGNORE INTO prices (coin_id, price)
        SELECT coin_id, CASE WHEN symbol = ? THEN 1.0 ELSE 1.0 END
        FROM coins
    """, (RESERVE_CURRENCY,))
    conn.commit()
    conn.close()
    print("✅ Database ready.")


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────

def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')

def compute_block_hash(prev_hash, trade_id, from_user_id, to_user_id, coin_id, amount, executed_at):
    data = f"{prev_hash}|{trade_id}|{from_user_id}|{to_user_id}|{coin_id}|{amount}|{executed_at}"
    return hashlib.sha256(data.encode()).hexdigest()

def get_reserve_id(db):
    r = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (RESERVE_CURRENCY,)).fetchone()
    return r['coin_id']


def get_implied_prices(db, window_seconds=60):
    """
    Estimate coin prices in CLASSUSD from matched transfer pairs:
    - one non-CLASSUSD transfer
    - one CLASSUSD transfer
    - same two users (either direction)
    - executed within `window_seconds`
    """
    reserve_id = get_reserve_id(db)

    # Potential matches (many-to-many). We'll greedily choose closest unique pairs.
    candidates = db.execute("""
        SELECT
            ct.trade_id  AS coin_trade_id,
            ct.coin_id   AS coin_id,
            cc.symbol    AS symbol,
            ct.amount    AS coin_amount,
            rt.trade_id  AS cash_trade_id,
            rt.amount    AS cash_amount,
            ABS(strftime('%s', ct.executed_at) - strftime('%s', rt.executed_at)) AS time_gap_s,
            CASE
                WHEN ct.executed_at >= rt.executed_at THEN ct.executed_at
                ELSE rt.executed_at
            END AS matched_at
        FROM trades ct
        JOIN coins cc ON cc.coin_id = ct.coin_id
        JOIN trades rt ON rt.coin_id = ?
        WHERE ct.trade_type = 'TRANSFER'
          AND rt.trade_type = 'TRANSFER'
          AND ct.coin_id != ?
          AND ct.from_user_id IS NOT NULL
          AND ct.to_user_id   IS NOT NULL
          AND rt.from_user_id IS NOT NULL
          AND rt.to_user_id   IS NOT NULL
          AND (
                (ct.from_user_id = rt.from_user_id AND ct.to_user_id = rt.to_user_id)
             OR (ct.from_user_id = rt.to_user_id   AND ct.to_user_id = rt.from_user_id)
          )
          AND ABS(strftime('%s', ct.executed_at) - strftime('%s', rt.executed_at)) <= ?
        ORDER BY time_gap_s ASC, ct.trade_id DESC, rt.trade_id DESC
    """, (reserve_id, reserve_id, int(window_seconds))).fetchall()

    used_coin_trades = set()
    used_cash_trades = set()
    implied_by_coin = {}  # coin_id -> stats
    for row in candidates:
        coin_tid = row['coin_trade_id']
        cash_tid = row['cash_trade_id']
        if coin_tid in used_coin_trades or cash_tid in used_cash_trades:
            continue
        used_coin_trades.add(coin_tid)
        used_cash_trades.add(cash_tid)

        coin_id = row['coin_id']
        stats = implied_by_coin.setdefault(coin_id, {
            'sum_coin': 0.0,
            'sum_cash': 0.0,
            'matches': 0,
            'updated_at': row['matched_at']
        })
        stats['sum_coin'] += float(row['coin_amount'] or 0.0)
        stats['sum_cash'] += float(row['cash_amount'] or 0.0)
        stats['matches'] += 1
        if row['matched_at'] and (not stats['updated_at'] or row['matched_at'] > stats['updated_at']):
            stats['updated_at'] = row['matched_at']

    base_rows = db.execute("""
        SELECT c.coin_id, c.symbol, c.name,
               COALESCE(p.price, 0.0) AS fallback_price,
               p.updated_at AS fallback_updated_at
        FROM coins c
        LEFT JOIN prices p ON p.coin_id = c.coin_id
        ORDER BY c.symbol
    """).fetchall()

    out = []
    for r in base_rows:
        coin_id = r['coin_id']
        symbol = r['symbol']
        if symbol == RESERVE_CURRENCY:
            out.append({
                'symbol': symbol,
                'name': r['name'],
                'price': 1.0,
                'method': 'reserve',
                'matches': 0,
                'updated_at': now_iso()
            })
            continue

        stats = implied_by_coin.get(coin_id)
        if stats and stats['sum_coin'] > 0:
            out.append({
                'symbol': symbol,
                'name': r['name'],
                'price': stats['sum_cash'] / stats['sum_coin'],
                'method': 'implied',
                'matches': stats['matches'],
                'updated_at': stats['updated_at']
            })
        else:
            out.append({
                'symbol': symbol,
                'name': r['name'],
                'price': float(r['fallback_price'] or 0.0),
                'method': 'fallback',
                'matches': 0,
                'updated_at': r['fallback_updated_at']
            })
    return out


def compute_portfolio_value(db, user_id):
    """Returns (cash_classusd, portfolio_value_classusd). Uses prices table."""
    # Get CLASSUSD coin_id + cash amount
    res = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (RESERVE_CURRENCY,)).fetchone()
    reserve_id = res['coin_id']
    cash, cash_staked = get_balance(db, user_id, reserve_id)
    cash_total = cash + cash_staked

    rows = db.execute("""
        SELECT b.coin_id, c.symbol, (b.amount + b.staked) AS units, p.price
        FROM balances b
        JOIN coins c ON c.coin_id=b.coin_id
        LEFT JOIN prices p ON p.coin_id=b.coin_id
        WHERE b.user_id=?
    """, (user_id,)).fetchall()

    total = 0.0
    for r in rows:
        units = float(r['units'] or 0.0)
        price = float(r['price'] or 0.0)
        # reserve currency price is 1 by definition
        if r['symbol'] == RESERVE_CURRENCY:
            total += units
        else:
            total += units * price
    return cash_total, total


def get_balance(db, user_id, coin_id):
    r = db.execute(
        "SELECT amount, staked FROM balances WHERE user_id=? AND coin_id=?",
        (user_id, coin_id)
    ).fetchone()
    return (r['amount'], r['staked']) if r else (0.0, 0.0)

def set_balance(db, user_id, coin_id, amount, staked=None):
    if staked is None:
        _, cur_staked = get_balance(db, user_id, coin_id)
        staked = cur_staked
    db.execute("""
        INSERT INTO balances (user_id, coin_id, amount, staked)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, coin_id)
        DO UPDATE SET amount=excluded.amount, staked=excluded.staked
    """, (user_id, coin_id, round(amount, 4), round(staked, 4)))

def get_prev_hash(db, coin_id):
    r = db.execute(
        "SELECT this_hash FROM ledger_blocks WHERE coin_id=? ORDER BY block_id DESC LIMIT 1",
        (coin_id,)
    ).fetchone()
    return r['this_hash'] if r else '0' * 64

def add_block(db, trade_id, coin_id, from_user_id, to_user_id, amount, executed_at):
    prev_hash  = get_prev_hash(db, coin_id)
    block_data = json.dumps({
        'trade_id': trade_id, 'coin_id': coin_id,
        'from_user_id': from_user_id, 'to_user_id': to_user_id,
        'amount': amount, 'executed_at': executed_at
    })
    this_hash = compute_block_hash(
        prev_hash, trade_id, from_user_id, to_user_id, coin_id, amount, executed_at
    )
    db.execute("""
        INSERT INTO ledger_blocks (coin_id, trade_id, prev_hash, block_data, this_hash)
        VALUES (?, ?, ?, ?, ?)
    """, (coin_id, trade_id, prev_hash, block_data, this_hash))
    return this_hash

def do_transfer(db, from_user_id, to_user_id, coin_id, amount,
                trade_type='transfer', note=''):
    """
    Core transfer function. Applies burn, anti-whale, records trade + block.
    Returns (trade_id, burned, received, error_string)
    """
    coin = db.execute("SELECT * FROM coins WHERE coin_id=?", (coin_id,)).fetchone()
    if not coin:
        return None, 0, 0, 'Coin not found.'

    burned   = round(amount * coin['burn_rate'], 4) if trade_type == 'transfer' else 0.0
    received = round(amount - burned, 4)

    # Anti-whale check
    if coin['max_holding'] and to_user_id:
        cur_bal, _ = get_balance(db, to_user_id, coin_id)
        if cur_bal + received > coin['max_holding']:
            return None, 0, 0, (
                f"Anti-whale limit: {coin['symbol']} holders can't exceed "
                f"{coin['max_holding']:.2f}. Recipient has {cur_bal:.2f}."
            )

    # Deduct from sender
    if from_user_id is not None:
        s_bal, s_staked = get_balance(db, from_user_id, coin_id)
        if s_bal < amount:
            return None, 0, 0, f'Insufficient balance. You have {s_bal:.4f} {coin["symbol"]}.'
        set_balance(db, from_user_id, coin_id, s_bal - amount)

    # Credit recipient
    if to_user_id is not None:
        r_bal, _ = get_balance(db, to_user_id, coin_id)
        set_balance(db, to_user_id, coin_id, r_bal + received)

    ts = now_iso()
    db.execute("""
        INSERT INTO trades
            (from_user_id, to_user_id, coin_id, amount, burned_amount, trade_type, note, executed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (from_user_id, to_user_id, coin_id, amount, burned, trade_type, note, ts))
    trade_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()['id']

    add_block(db, trade_id, coin_id, from_user_id, to_user_id, amount, ts)
    return trade_id, burned, received, None


# ─────────────────────────────────────────
#  AUTH
# ─────────────────────────────────────────

@app.route('/api/register', methods=['POST'])
def register():
    d      = request.json
    handle = (d.get('username') or '').strip().lower()
    pw     = d.get('password') or ''

    if not handle or not pw:
        return jsonify(error='Username and password required.'), 400
    if len(handle) < 2 or len(handle) > 20:
        return jsonify(error='Username must be 2–20 characters.'), 400
    if not all(c.isalnum() or c == '_' for c in handle):
        return jsonify(error='Letters, numbers, underscores only.'), 400

    db = get_db()
    if db.execute("SELECT 1 FROM users WHERE handle=?", (handle,)).fetchone():
        return jsonify(error='Username already taken.'), 400

    db.execute("INSERT INTO users (handle, password_hash) VALUES (?,?)",
               (handle, hash_password(pw)))
    db.commit()
    user_id  = db.execute("SELECT user_id FROM users WHERE handle=?", (handle,)).fetchone()['user_id']
    coin_id  = get_reserve_id(db)

    _, _, _, err = do_transfer(db, None, user_id, coin_id, STARTING_BALANCE,
                               'welcome', 'Welcome bonus!')
    if err:
        return jsonify(error=err), 500
    db.commit()
    return jsonify(success=True, username=handle)


@app.route('/api/login', methods=['POST'])
def login():
    d      = request.json
    handle = (d.get('username') or '').strip().lower()
    pw     = d.get('password') or ''
    db     = get_db()
    row    = db.execute("SELECT user_id, password_hash FROM users WHERE handle=?", (handle,)).fetchone()
    if not row or row['password_hash'] != hash_password(pw):
        return jsonify(error='Invalid username or password.'), 401
    return jsonify(success=True, username=handle)


# ─────────────────────────────────────────
#  COINS
# ─────────────────────────────────────────

@app.route('/api/coins', methods=['GET'])
def list_coins():
    db   = get_db()
    rows = db.execute("""
        SELECT c.*, u.handle AS creator_name
        FROM coins c
        LEFT JOIN users u ON c.creator_id = u.user_id
        ORDER BY c.created_at DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/coins/create', methods=['POST'])
def create_coin():
    d        = request.json
    handle   = (d.get('username') or '').strip().lower()
    pw       = d.get('password') or ''
    symbol   = (d.get('symbol') or '').strip().upper()
    name     = (d.get('name') or '').strip()
    supply   = d.get('total_supply', 1000)
    burn     = d.get('burn_rate', 0.0)
    airdrop  = d.get('airdrop_amount', 0.0)
    max_hold = d.get('max_holding')      # None = no limit
    staking  = 1 if d.get('staking_enabled') else 0
    desc     = (d.get('description') or '').strip()

    # Validate
    if not symbol or not name:
        return jsonify(error='Symbol and name required.'), 400
    if len(symbol) < 2 or len(symbol) > 8:
        return jsonify(error='Symbol must be 2–8 characters.'), 400
    if not symbol.isalpha():
        return jsonify(error='Symbol: letters only.'), 400
    if len(name) > 40:
        return jsonify(error='Name too long (max 40 chars).'), 400
    try:
        supply = float(supply)
        burn   = float(burn)
        if supply <= 0 or supply > 1_000_000:
            raise ValueError
        if burn < 0 or burn > 0.5:
            raise ValueError
    except:
        return jsonify(error='Invalid supply or burn rate.'), 400

    db  = get_db()
    row = db.execute("SELECT user_id, password_hash FROM users WHERE handle=?", (handle,)).fetchone()
    if not row or row['password_hash'] != hash_password(pw):
        return jsonify(error='Invalid credentials.'), 401

    user_id = row['user_id']

    # One coin per student
    if db.execute("SELECT 1 FROM coins WHERE creator_id=?", (user_id,)).fetchone():
        return jsonify(error='You already have a coin. Each student gets one.'), 400
    if db.execute("SELECT 1 FROM coins WHERE symbol=?", (symbol,)).fetchone():
        return jsonify(error=f'Symbol {symbol} already taken.'), 400

    db.execute("""
        INSERT INTO coins
            (symbol, name, creator_id, total_supply, burn_rate,
             airdrop_amount, max_holding, staking_enabled, description)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (symbol, name, user_id, supply, burn,
          airdrop, max_hold, staking, desc))
    db.commit()

    coin_id = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (symbol,)).fetchone()['coin_id']

    # Creator gets the full supply
    set_balance(db, user_id, coin_id, supply)
    ts = now_iso()
    db.execute("""
        INSERT INTO trades (from_user_id, to_user_id, coin_id, amount, burned_amount, trade_type, note, executed_at)
        VALUES (NULL, ?, ?, ?, 0, 'welcome', 'Initial coin supply', ?)
    """, (user_id, coin_id, supply, ts))
    trade_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()['id']
    add_block(db, trade_id, coin_id, None, user_id, supply, ts)
    db.commit()

    return jsonify(success=True, symbol=symbol, coin_id=coin_id)


# ─────────────────────────────────────────
#  TRADING
# ─────────────────────────────────────────

@app.route('/api/send', methods=['POST'])
def send():
    d           = request.json
    from_handle = (d.get('from') or '').strip().lower()
    to_handle   = (d.get('to')   or '').strip().lower()
    pw          = d.get('password') or ''
    symbol      = (d.get('symbol') or RESERVE_CURRENCY).strip().upper()
    note        = (d.get('note') or '').strip()

    try:
        amount = round(float(d.get('amount', 0)), 4)
    except:
        return jsonify(error='Invalid amount.'), 400

    if amount <= 0:
        return jsonify(error='Amount must be positive.'), 400
    if from_handle == to_handle:
        return jsonify(error='Cannot send to yourself.'), 400

    db     = get_db()
    sender = db.execute("SELECT user_id, password_hash FROM users WHERE handle=?", (from_handle,)).fetchone()
    if not sender or sender['password_hash'] != hash_password(pw):
        return jsonify(error='Invalid credentials.'), 401

    recip = db.execute("SELECT user_id FROM users WHERE handle=?", (to_handle,)).fetchone()
    if not recip:
        return jsonify(error=f'User "{to_handle}" not found.'), 404

    coin = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (symbol,)).fetchone()
    if not coin:
        return jsonify(error=f'Coin "{symbol}" not found.'), 404

    trade_id, burned, received, err = do_transfer(
        db, sender['user_id'], recip['user_id'], coin['coin_id'], amount, 'transfer', note
    )
    if err:
        return jsonify(error=err), 400

    db.commit()
    return jsonify(success=True, trade_id=trade_id, burned=burned, received=received)


# ─────────────────────────────────────────
#  TOKEN MECHANICS
# ─────────────────────────────────────────

@app.route('/api/airdrop', methods=['POST'])
def airdrop():
    """Coin creator drops airdrop_amount to every holder of their coin."""
    d      = request.json
    handle = (d.get('username') or '').strip().lower()
    pw     = d.get('password') or ''
    symbol = (d.get('symbol') or '').strip().upper()

    db     = get_db()
    user   = db.execute("SELECT user_id, password_hash FROM users WHERE handle=?", (handle,)).fetchone()
    if not user or user['password_hash'] != hash_password(pw):
        return jsonify(error='Invalid credentials.'), 401

    coin = db.execute("""
        SELECT * FROM coins WHERE symbol=? AND creator_id=?
    """, (symbol, user['user_id'])).fetchone()
    if not coin:
        return jsonify(error='Coin not found or you are not the creator.'), 403
    if coin['airdrop_amount'] <= 0:
        return jsonify(error='This coin has no airdrop amount configured.'), 400

    # Get all holders (excluding creator to avoid self-airdrop)
    holders = db.execute("""
        SELECT user_id FROM balances
        WHERE coin_id=? AND user_id != ? AND amount > 0
    """, (coin['coin_id'], user['user_id'])).fetchall()

    if not holders:
        return jsonify(error='No other holders yet — send your coin to classmates first.'), 400

    count = 0
    for h in holders:
        _, _, _, err = do_transfer(
            db, None, h['user_id'], coin['coin_id'],
            coin['airdrop_amount'], 'airdrop', f'Airdrop from {handle}'
        )
        if not err:
            count += 1

    db.commit()
    return jsonify(success=True, recipients=count,
                   total_dropped=count * coin['airdrop_amount'])


@app.route('/api/stake', methods=['POST'])
def stake():
    """Stake or unstake coins. Claim staking rewards."""
    d      = request.json
    handle = (d.get('username') or '').strip().lower()
    pw     = d.get('password') or ''
    symbol = (d.get('symbol') or '').strip().upper()
    action = d.get('action', 'stake')  # 'stake', 'unstake', 'claim'

    try:
        amount = round(float(d.get('amount', 0)), 4) if action != 'claim' else 0
    except:
        return jsonify(error='Invalid amount.'), 400

    db   = get_db()
    user = db.execute("SELECT user_id, password_hash FROM users WHERE handle=?", (handle,)).fetchone()
    if not user or user['password_hash'] != hash_password(pw):
        return jsonify(error='Invalid credentials.'), 401

    coin = db.execute("SELECT * FROM coins WHERE symbol=?", (symbol,)).fetchone()
    if not coin:
        return jsonify(error='Coin not found.'), 404
    if not coin['staking_enabled']:
        return jsonify(error=f'{symbol} does not support staking.'), 400

    uid     = user['user_id']
    cid     = coin['coin_id']
    bal, staked = get_balance(db, uid, cid)

    if action == 'stake':
        if amount <= 0: return jsonify(error='Enter amount to stake.'), 400
        if bal < amount: return jsonify(error=f'Not enough {symbol} to stake.'), 400
        set_balance(db, uid, cid, bal - amount, staked + amount)
        db.execute("UPDATE balances SET last_staked=? WHERE user_id=? AND coin_id=?",
                   (now_iso(), uid, cid))
        db.commit()
        return jsonify(success=True, staked=staked + amount)

    elif action == 'unstake':
        if amount <= 0: return jsonify(error='Enter amount to unstake.'), 400
        if staked < amount: return jsonify(error=f'You only have {staked:.4f} staked.'), 400
        set_balance(db, uid, cid, bal + amount, staked - amount)
        db.commit()
        return jsonify(success=True, staked=staked - amount)

    elif action == 'claim':
        if staked <= 0: return jsonify(error='Nothing staked.'), 400
        reward = round(staked * STAKING_RATE, 4)
        _, _, _, err = do_transfer(db, None, uid, cid, reward, 'stake_reward',
                                   f'Staking reward ({STAKING_RATE*100:.0f}% of {staked:.2f} staked)')
        if err: return jsonify(error=err), 400
        db.commit()
        return jsonify(success=True, reward=reward)

    return jsonify(error='Unknown action.'), 400


# ─────────────────────────────────────────
#  DATA ENDPOINTS
# ─────────────────────────────────────────

@app.route('/api/wallet/<username>')
def wallet(username):
    db   = get_db()
    user = db.execute("SELECT user_id FROM users WHERE handle=?", (username.lower(),)).fetchone()
    if not user: return jsonify(error='User not found.'), 404
    uid = user['user_id']

    # All balances for this user
    bals = db.execute("""
        SELECT c.symbol, c.name, c.burn_rate, c.staking_enabled,
               c.max_holding, b.amount, b.staked
        FROM balances b
        JOIN coins c ON b.coin_id = c.coin_id
        WHERE b.user_id = ?
        ORDER BY c.symbol
    """, (uid,)).fetchall()

    # Recent trades (all coins)
    trades = db.execute("""
        SELECT t.trade_id,
               COALESCE(s.handle,'SYSTEM') AS from_user,
               r.handle AS to_user,
               c.symbol, t.amount, t.burned_amount, t.trade_type, t.note, t.executed_at
        FROM trades t
        LEFT JOIN users s ON t.from_user_id = s.user_id
        JOIN  users r ON t.to_user_id = r.user_id
        JOIN  coins c ON t.coin_id = c.coin_id
        WHERE t.from_user_id=? OR t.to_user_id=?
        ORDER BY t.executed_at DESC LIMIT 60
    """, (uid, uid)).fetchall()

    # My coin (if any)
    my_coin = db.execute("""
        SELECT c.*, u.handle AS creator_name
        FROM coins c LEFT JOIN users u ON c.creator_id = u.user_id
        WHERE c.creator_id=?
    """, (uid,)).fetchone()

    return jsonify(
        username=username.lower(),
        balances=[dict(b) for b in bals],
        transactions=[dict(t) for t in trades],
        my_coin=dict(my_coin) if my_coin else None
    )


@app.route('/api/leaderboard')
def leaderboard():
    symbol = request.args.get('symbol', RESERVE_CURRENCY).upper()
    db     = get_db()
    rows   = db.execute("""
        SELECT u.handle, b.amount, b.staked
        FROM balances b
        JOIN users u ON b.user_id = u.user_id
        JOIN coins c ON b.coin_id = c.coin_id
        WHERE c.symbol = ?
        ORDER BY b.amount DESC
    """, (symbol,)).fetchall()
    return jsonify([dict(r) for r in rows])



@app.route('/api/prices')
def api_prices():
    db = get_db()
    rows = get_implied_prices(db, window_seconds=60)
    return jsonify(rows)


@app.route('/api/prices/set', methods=['POST'])
def api_prices_set():
    """Teacher sets reference prices (in CLASSUSD)."""
    d = request.json or {}
    key = d.get('teacher_key') or ''
    if TEACHER_KEY and key != TEACHER_KEY:
        return jsonify(error='Invalid teacher key.'), 401

    symbol = (d.get('symbol') or '').strip().upper()
    try:
        price = float(d.get('price', 0))
    except:
        return jsonify(error='Invalid price.'), 400
    if price < 0:
        return jsonify(error='Price must be >= 0.'), 400

    db = get_db()
    coin = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (symbol,)).fetchone()
    if not coin:
        return jsonify(error='Coin not found.'), 404

    db.execute("""
        INSERT INTO prices(coin_id, price, updated_at)
        VALUES (?,?,?)
        ON CONFLICT(coin_id) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at
    """, (coin['coin_id'], price, now_iso()))
    db.commit()
    return jsonify(success=True, symbol=symbol, price=price)


@app.route('/api/portfolio_leaderboard')
def api_portfolio_leaderboard():
    """Leaderboard ranked by implied-valuation portfolio value in CLASSUSD."""
    db = get_db()
    price_rows = get_implied_prices(db, window_seconds=60)
    price_map = {p['symbol']: float(p.get('price') or 0.0) for p in price_rows}
    users = db.execute("SELECT user_id, handle FROM users").fetchall()
    out = []
    for u in users:
        bals = db.execute("""
            SELECT c.symbol, (b.amount + b.staked) AS units
            FROM balances b
            JOIN coins c ON c.coin_id = b.coin_id
            WHERE b.user_id=?
        """, (u['user_id'],)).fetchall()
        total = 0.0
        cash = 0.0
        for b in bals:
            symbol = b['symbol']
            units = float(b['units'] or 0.0)
            if symbol == RESERVE_CURRENCY:
                cash += units
                total += units
            else:
                total += units * float(price_map.get(symbol, 0.0))
        out.append({"username": u["handle"], "cash": round(cash,2), "portfolio": round(total,2)})
    out.sort(key=lambda x: x["portfolio"], reverse=True)
    return jsonify(out)


@app.route('/api/ledger')
def ledger():
    symbol = request.args.get('symbol', '').upper()
    db     = get_db()
    query  = """
        SELECT t.trade_id, lb.block_id, COALESCE(s.handle,'SYSTEM') AS from_user,
               r.handle AS to_user, c.symbol, t.amount,
               t.burned_amount, t.trade_type, t.note, t.executed_at,
               lb.this_hash AS block_hash
        FROM trades t
        LEFT JOIN users s ON t.from_user_id = s.user_id
        JOIN  users r ON t.to_user_id = r.user_id
        JOIN  coins c ON t.coin_id = c.coin_id
        LEFT JOIN ledger_blocks lb ON lb.trade_id = t.trade_id
        {where}
        ORDER BY t.executed_at DESC
    """
    if symbol:
        rows = db.execute(query.format(where="WHERE c.symbol=?"), (symbol,)).fetchall()
    else:
        rows = db.execute(query.format(where="")).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/chain')
def chain():
    symbol = request.args.get('symbol', RESERVE_CURRENCY).upper()
    db     = get_db()
    coin   = db.execute("SELECT coin_id FROM coins WHERE symbol=?", (symbol,)).fetchone()
    if not coin: return jsonify(error='Coin not found.'), 404

    blocks = db.execute("""
        SELECT lb.*, COALESCE(s.handle,'SYSTEM') AS from_user, r.handle AS to_user,
               t.amount, t.trade_type, t.executed_at AS trade_time
        FROM ledger_blocks lb
        JOIN  trades t ON lb.trade_id = t.trade_id
        LEFT JOIN users s ON t.from_user_id = s.user_id
        JOIN  users r ON t.to_user_id = r.user_id
        WHERE lb.coin_id=?
        ORDER BY lb.block_id ASC
    """, (coin['coin_id'],)).fetchall()

    blocks = [dict(b) for b in blocks]
    valid, bad = True, None
    for i, b in enumerate(blocks):
        expected_prev = blocks[i-1]['this_hash'] if i > 0 else '0' * 64
        if b['prev_hash'] != expected_prev:
            valid, bad = False, b['block_id']
            break
        expected_hash = compute_block_hash(
            b['prev_hash'], b['trade_id'], None if b['from_user'] == 'SYSTEM' else
            db.execute("SELECT user_id FROM users WHERE handle=?", (b['from_user'],)).fetchone()['user_id'],
            db.execute("SELECT user_id FROM users WHERE handle=?", (b['to_user'],)).fetchone()['user_id'],
            coin['coin_id'], b['amount'], b['trade_time']
        )
        if b['this_hash'] != expected_hash:
            valid, bad = False, b['block_id']
            break

    return jsonify(is_valid=valid, first_bad_block=bad, blocks=blocks)


@app.route('/api/users')
def users():
    db = get_db()
    return jsonify([r['handle'] for r in db.execute("SELECT handle FROM users ORDER BY handle").fetchall()])


# ─────────────────────────────────────────
#  EXPORTS
# ─────────────────────────────────────────

@app.route('/api/export/csv')
def export_csv():
    db   = get_db()
    rows = db.execute("""
        SELECT t.trade_id, COALESCE(s.handle,'SYSTEM') AS from_user,
               r.handle AS to_user, c.symbol, t.amount, t.burned_amount,
               t.trade_type, t.note, t.executed_at, lb.prev_hash, lb.this_hash
        FROM trades t
        LEFT JOIN users s ON t.from_user_id = s.user_id
        JOIN  users r ON t.to_user_id = r.user_id
        JOIN  coins c ON t.coin_id = c.coin_id
        LEFT JOIN ledger_blocks lb ON lb.trade_id = t.trade_id
        ORDER BY t.trade_id
    """).fetchall()
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(['trade_id','from_user','to_user','symbol','amount','burned_amount',
                'trade_type','note','executed_at','prev_hash','this_hash'])
    for r in rows: w.writerow(list(r))
    out.seek(0)
    return send_file(io.BytesIO(out.getvalue().encode()), mimetype='text/csv',
                     as_attachment=True, download_name='classusd_trades.csv')


@app.route('/api/export/db')
def export_db():
    return send_file(os.path.abspath(DB_PATH), mimetype='application/octet-stream',
                     as_attachment=True, download_name='classusd.db')


@app.route('/api/export/schema')
def export_schema():
    db   = get_db()
    rows = db.execute("""
        SELECT sql FROM sqlite_master
        WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name
    """).fetchall()
    return '\n\n'.join(r['sql'] + ';' for r in rows), 200, \
           {'Content-Type': 'text/plain; charset=utf-8'}


# ─────────────────────────────────────────
#  STATIC
# ─────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('public', 'index.html')


if __name__ == '__main__':
    init_db()
    import socket
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except:
        local_ip = 'your-ip-here'
    print(f"\n{'='*52}")
    print(f"  CLASSUSD Exchange  |  Local: http://localhost:5000")
    print(f"  Students connect:     http://{local_ip}:5000")
    print(f"  DB: {os.path.abspath(DB_PATH)}")
    print(f"{'='*52}\n")
    app.run(host='0.0.0.0', port=5000, debug=False)
