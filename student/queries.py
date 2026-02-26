"""
CLASSUSD — Student SQL Query Script (v3)
=========================================
Run with: python queries.py

Download the latest database from http://[teacher-ip]:5000 (⬇ DB button)
Put classusd.db in the same folder as this file, then run.

The database has these tables:
  users         — every student account
  coins         — every coin created (including CLASSUSD)
  balances      — what each user holds of each coin
  trades        — every transaction (send, airdrop, stake reward, welcome)
  ledger_blocks — blockchain blocks, one per trade, per coin

And these views (pre-built for you):
  trade_history — human-readable version of trades with JOIN already done
  leaderboard   — balances with username and symbol
"""

import sqlite3

DB_PATH = 'classusd.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur  = conn.cursor()

print("Connected to", DB_PATH)
print("=" * 60)


# ════════════════════════════════════════════════════════════
# QUERY 1 — SELECT with WHERE
# List all trades where amount > 10 for a specific coin
# ════════════════════════════════════════════════════════════

print("\n── Query 1: Large Trades (SELECT + WHERE) ──")

cur.execute("""
    SELECT
        trade_id,
        from_user,
        to_user,
        symbol,
        amount,
        executed_at
    FROM trade_history
    WHERE amount > 10
    ORDER BY amount DESC
""")

rows = cur.fetchall()
if rows:
    print(f"{'ID':<5} {'From':<15} {'To':<15} {'Coin':<10} {'Amount':<10} {'When'}")
    print("-" * 70)
    for r in rows:
        print(f"{r['trade_id']:<5} {r['from_user']:<15} {r['to_user']:<15} {r['symbol']:<10} {r['amount']:<10.4f} {r['executed_at']}")
else:
    print("No trades over 10 yet.")


# ════════════════════════════════════════════════════════════
# QUERY 2 — JOIN across 2+ tables
# Show each user's balance of each coin with coin details
# ════════════════════════════════════════════════════════════

print("\n── Query 2: Portfolio Report (JOIN) ──")

cur.execute("""
    SELECT
        u.handle                     AS username,
        c.symbol,
        c.name                       AS coin_name,
        c.burn_rate,
        b.amount                     AS balance,
        b.staked
    FROM balances b
    JOIN users u ON b.user_id = u.user_id
    JOIN coins c ON b.coin_id = c.coin_id
    WHERE b.amount > 0 OR b.staked > 0
    ORDER BY u.handle, c.symbol
""")

rows = cur.fetchall()
if rows:
    print(f"{'User':<18} {'Coin':<10} {'Balance':>12} {'Staked':>10} {'Burn Rate':>10}")
    print("-" * 60)
    for r in rows:
        print(f"{r['username']:<18} {r['symbol']:<10} {r['balance']:>12.4f} {r['staked']:>10.4f} {r['burn_rate']*100:>9.1f}%")
else:
    print("No balances yet.")


# ════════════════════════════════════════════════════════════
# QUERY 3 — GROUP BY + ORDER BY
# Trading volume per coin (total amount moved)
# ════════════════════════════════════════════════════════════

print("\n── Query 3: Coin Volume Leaderboard (GROUP BY + ORDER BY) ──")

cur.execute("""
    SELECT
        c.symbol,
        c.name,
        COUNT(t.trade_id)        AS trade_count,
        SUM(t.amount)            AS total_volume,
        SUM(t.burned_amount)     AS total_burned,
        AVG(t.amount)            AS avg_trade_size
    FROM trades t
    JOIN coins c ON t.coin_id = c.coin_id
    GROUP BY c.coin_id
    ORDER BY total_volume DESC
""")

rows = cur.fetchall()
if rows:
    print(f"{'Symbol':<10} {'Name':<20} {'Trades':>7} {'Volume':>12} {'Burned':>10} {'Avg':>8}")
    print("-" * 75)
    for r in rows:
        print(f"{r['symbol']:<10} {r['name']:<20} {r['trade_count']:>7} {r['total_volume']:>12.4f} {r['total_burned']:>10.4f} {r['avg_trade_size']:>8.4f}")
else:
    print("No trades yet.")


# ════════════════════════════════════════════════════════════
# QUERY 4 — HAVING (filter aggregated groups)
# Find coins that have had MORE than 3 trades
# ════════════════════════════════════════════════════════════

print("\n── Query 4: Active Coins (HAVING) ──")

cur.execute("""
    SELECT
        c.symbol,
        c.name,
        COUNT(t.trade_id) AS trade_count
    FROM trades t
    JOIN coins c ON t.coin_id = c.coin_id
    GROUP BY c.coin_id
    HAVING COUNT(t.trade_id) > 3
    ORDER BY trade_count DESC
""")

rows = cur.fetchall()
if rows:
    print(f"{'Symbol':<10} {'Name':<25} {'# Trades'}")
    print("-" * 45)
    for r in rows:
        print(f"{r['symbol']:<10} {r['name']:<25} {r['trade_count']}")
else:
    print("No coins with more than 3 trades yet — keep trading!")


# ════════════════════════════════════════════════════════════
# CHALLENGE: Token mechanics analysis
# ════════════════════════════════════════════════════════════

print("\n── Challenge: Token Mechanics Summary ──")

cur.execute("""
    SELECT
        c.symbol,
        c.burn_rate,
        c.airdrop_amount,
        c.max_holding,
        c.staking_enabled,
        u.handle AS creator,
        c.total_supply,
        COALESCE(SUM(t.burned_amount), 0) AS total_burned_so_far,
        COUNT(CASE WHEN t.trade_type = 'airdrop' THEN 1 END) AS airdrop_events,
        COUNT(CASE WHEN t.trade_type = 'stake_reward' THEN 1 END) AS stake_rewards_paid
    FROM coins c
    LEFT JOIN users u ON c.creator_id = u.user_id
    LEFT JOIN trades t ON t.coin_id = c.coin_id
    GROUP BY c.coin_id
    ORDER BY c.created_at
""")

rows = cur.fetchall()
for r in rows:
    rules = []
    if r['burn_rate'] > 0:       rules.append(f"🔥 Burn {r['burn_rate']*100:.1f}%")
    if r['airdrop_amount'] > 0:  rules.append(f"🎁 Airdrop {r['airdrop_amount']}")
    if r['max_holding']:         rules.append(f"🐋 Max {r['max_holding']}")
    if r['staking_enabled']:     rules.append("🧊 Staking")

    print(f"\n  {r['symbol']} ({r['creator'] or 'SYSTEM'})")
    print(f"    Rules:          {', '.join(rules) if rules else 'None'}")
    print(f"    Total Supply:   {r['total_supply']:,.0f}")
    print(f"    Burned So Far:  {r['total_burned_so_far']:.4f}")
    print(f"    Airdrops Run:   {r['airdrop_events']}")
    print(f"    Stake Payouts:  {r['stake_rewards_paid']}")


conn.close()
print("\n" + "=" * 60)
print("Done! Add your own queries below — try JOINing different tables.")
print("Tip: run 'python -c \"import sqlite3; conn=sqlite3.connect('classusd.db'); [print(r) for r in conn.execute(\\\"SELECT name FROM sqlite_master WHERE type=\\'table\\'\\\")]\"' to see all tables.")
