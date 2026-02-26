"""
╔══════════════════════════════════════════════════════════╗
║         CLASSUSD — SQL Lab Starter Code                  ║
║         IB Computer Science — HL Year 1                  ║
╠══════════════════════════════════════════════════════════╣
║  HOW TO USE THIS FILE                                    ║
║  1. Put classusd.db in the SAME folder as this file      ║
║  2. Open this folder in VS Code                          ║
║  3. Open a terminal: View → Terminal                     ║
║  4. Run:  python lab.py                                  ║
║  5. Screenshot your output for each section              ║
╚══════════════════════════════════════════════════════════╝

YOUR DETAILS — fill these in:
"""

MY_USERNAME = "your_username_here"   # ← change this to YOUR exchange username
MY_COIN     = "YOUR_SYMBOL"          # ← change this to YOUR coin's ticker symbol


# ═══════════════════════════════════════════════════════════
#  SETUP — don't change anything in this section
#  (This is the "boilerplate" — the connection code you get
#   for free. In Java this would be like your import + main)
# ═══════════════════════════════════════════════════════════

import sqlite3
import os

# Find the database file
DB_FILE = "classusd.db"
if not os.path.exists(DB_FILE):
    print(f"ERROR: Cannot find '{DB_FILE}'")
    print(f"Make sure classusd.db is in the same folder as this file.")
    print(f"Download it from the exchange site using the ⬇ DB button.")
    exit()

# Connect to the database
conn = sqlite3.connect(DB_FILE)
conn.row_factory = sqlite3.Row   # lets you access columns by name (like row["amount"])
cur  = conn.cursor()

print("=" * 55)
print("  CLASSUSD SQL Lab")
print(f"  Student: {MY_USERNAME}  |  Coin: {MY_COIN}")
print("=" * 55)


# ── HELPER FUNCTION ─────────────────────────────────────
# This prints any query result as a neat table.
# You don't need to change this — just call run_query()

def run_query(label, sql, params=()):
    """
    Runs a SQL query and prints the results as a table.
    
    Args:
        label  — a title string for this query
        sql    — your SQL query as a string
        params — optional tuple of values to safely insert into the query
    """
    print(f"\n{'─' * 55}")
    print(f"  {label}")
    print(f"{'─' * 55}")
    
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        
        if not rows:
            print("  (no results — check your username/coin or run more trades)")
            return
        
        # Print column headers
        headers = rows[0].keys()
        col_widths = {h: max(len(str(h)), max(len(str(r[h] or "")) for r in rows)) for h in headers}
        
        header_line = "  " + "  ".join(str(h).ljust(col_widths[h]) for h in headers)
        print(header_line)
        print("  " + "-" * (len(header_line) - 2))
        
        # Print each row
        for row in rows:
            print("  " + "  ".join(str(row[h] or "").ljust(col_widths[h]) for h in headers))
        
        print(f"\n  ({len(rows)} row{'s' if len(rows) != 1 else ''} returned)")
        
    except sqlite3.Error as e:
        print(f"  SQL ERROR: {e}")
        print(f"  Check your query for typos.")


# ═══════════════════════════════════════════════════════════
#  SECTION 1 — Exploring the Database
#  
#  Think of each table like an ArrayList<Object> in Java,
#  except instead of objects, rows have named columns.
#  SELECT = deciding which columns you want
#  FROM   = which table to look in
#  WHERE  = like an if-statement filter on every row
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 1 — Exploring the Database                 ║")
print("╚══════════════════════════════════════════════════════╝")


# ── WORKED EXAMPLE 1A ───────────────────────────────────
# This one is done for you. Read it carefully — the next
# ones follow the same pattern.
#
# SQL equivalent of:
#   for (User u : users) { print(u.handle, u.created_at); }

run_query(
    "EXAMPLE 1A: All students in the class",
    """
    SELECT
        user_id,
        handle,
        created_at
    FROM users
    ORDER BY created_at ASC
    """
)


# ── TASK 1B — Your turn ──────────────────────────────────
# Write a query that shows ALL coins in the database.
# Include: symbol, name, total_supply, burn_rate, creator_id
# Order by total_supply from largest to smallest.
#
# Hint: the table is called 'coins'
# Hint: ORDER BY column DESC  means largest first

run_query(
    "TASK 1B: All coins (YOUR QUERY HERE)",
    """
    SELECT
        -- write your column names here, separated by commas
        -- ???
    FROM ???
    ORDER BY ??? DESC
    """
)


# ── TASK 1C — Filter with WHERE ──────────────────────────
# Write a query that shows only YOUR trades.
# The trades table has: trade_id, from_user_id, to_user_id,
#                       amount, burned_amount, trade_type, executed_at
#
# You need to find trades where to_user_id matches YOUR user_id.
# But you don't know your user_id number — use a subquery:
#
#   WHERE to_user_id = (SELECT user_id FROM users WHERE handle = ?)
#
# The ? is a safe placeholder — we pass MY_USERNAME as the value.
# This is like a method parameter in Java — never put usernames
# directly in the SQL string (that's called SQL injection!).

run_query(
    "TASK 1C: All trades YOU received",
    """
    SELECT
        trade_id,
        from_user_id,
        to_user_id,
        amount,
        trade_type,
        executed_at
    FROM trades
    WHERE to_user_id = (SELECT user_id FROM users WHERE handle = ?)
    ORDER BY executed_at DESC
    """,
    (MY_USERNAME,)   # ← this is how you pass the ? value safely
)


# ═══════════════════════════════════════════════════════════
#  SECTION 2 — JOIN: combining tables
#
#  The trades table stores user_id numbers, not names.
#  To get names you have to JOIN the users table.
#
#  Think of it like:
#    trade.from_user_id → users table → handle
#
#  In Java you'd do:  users.get(trade.fromUserId).handle
#  In SQL you JOIN:   JOIN users sender ON trade.from_user_id = sender.user_id
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 2 — JOIN: Combining Tables                 ║")
print("╚══════════════════════════════════════════════════════╝")


# ── WORKED EXAMPLE 2A ───────────────────────────────────
# This JOIN makes trades human-readable by replacing IDs with names.
# Notice: we JOIN users TWICE — once for sender, once for receiver.
# We give each one an alias (sender / receiver) to tell them apart.
# LEFT JOIN for sender = keep the row even if sender is NULL (SYSTEM trades)

run_query(
    "EXAMPLE 2A: Trade history with real names",
    """
    SELECT
        t.trade_id,
        COALESCE(sender.handle, 'SYSTEM')  AS from_user,
        receiver.handle                     AS to_user,
        c.symbol                            AS coin,
        t.amount,
        t.burned_amount,
        t.trade_type,
        t.executed_at
    FROM   trades t
    LEFT JOIN users sender   ON t.from_user_id = sender.user_id
    JOIN      users receiver ON t.to_user_id   = receiver.user_id
    JOIN      coins c        ON t.coin_id       = c.coin_id
    ORDER BY t.executed_at DESC
    LIMIT 10
    """
)


# ── TASK 2B — JOIN for balances ──────────────────────────
# Write a query that shows every student's balance of EVERY coin.
# Use human-readable names (handle, symbol) — not ID numbers.
# Tables you need: balances, users, coins
# Only show rows where amount > 0
# Order by symbol, then by amount descending

run_query(
    "TASK 2B: All balances with real names (YOUR QUERY HERE)",
    """
    SELECT
        -- u.handle AS username,
        -- c.symbol,
        -- b.amount,
        -- b.staked
        ???
    FROM balances b
    JOIN ??? ON ???
    JOIN ??? ON ???
    WHERE ???
    ORDER BY ???
    """
)


# ── TASK 2C — JOIN filtered to YOUR coin ─────────────────
# Copy your query from 2B but add a WHERE clause to only show
# balances for YOUR coin (use MY_COIN and a ? placeholder)

run_query(
    "TASK 2C: Who holds YOUR coin? (YOUR QUERY HERE)",
    """
    ???
    """,
    (MY_COIN,)   # pass MY_COIN as the ? value
)


# ═══════════════════════════════════════════════════════════
#  SECTION 3 — GROUP BY: Aggregating data
#
#  GROUP BY is like a dictionary/HashMap in Java where you
#  group rows by a key and run a function on each group.
#
#  SQL aggregate functions:
#    COUNT(*) — how many rows in the group
#    SUM(col) — add up all values in the group
#    AVG(col) — average of all values
#    MAX(col) — largest value
#    MIN(col) — smallest value
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 3 — GROUP BY: Aggregating Data             ║")
print("╚══════════════════════════════════════════════════════╝")


# ── WORKED EXAMPLE 3A ───────────────────────────────────
# Trading volume per coin — groups all trades by coin,
# then counts and sums within each group.

run_query(
    "EXAMPLE 3A: Trading volume per coin",
    """
    SELECT
        c.symbol,
        c.name,
        COUNT(t.trade_id)      AS trade_count,
        SUM(t.amount)          AS total_volume,
        SUM(t.burned_amount)   AS total_burned,
        ROUND(AVG(t.amount),2) AS avg_trade_size
    FROM   trades t
    JOIN   coins c ON t.coin_id = c.coin_id
    GROUP BY c.coin_id
    ORDER BY total_volume DESC
    """
)


# ── TASK 3B — Most active senders ────────────────────────
# Write a GROUP BY query that shows, for each sender:
#   - their handle (use COALESCE for SYSTEM trades)
#   - how many trades they sent (COUNT)
#   - total amount they sent (SUM)
#   - average trade size (AVG, rounded to 2 decimal places)
# Order by total amount sent, largest first.

run_query(
    "TASK 3B: Most active senders (YOUR QUERY HERE)",
    """
    SELECT
        ???
    FROM trades t
    LEFT JOIN users sender ON t.from_user_id = sender.user_id
    GROUP BY ???
    ORDER BY ???
    """
)


# ── TASK 3C — YOUR coin's stats ──────────────────────────
# Write a query that shows stats for ONLY your coin:
#   symbol, trade_count, total_volume, total_burned
# Filter to just MY_COIN using WHERE before the GROUP BY.

run_query(
    "TASK 3C: Stats for YOUR coin only (YOUR QUERY HERE)",
    """
    ???
    """,
    (MY_COIN,)
)


# ═══════════════════════════════════════════════════════════
#  SECTION 4 — HAVING: Filtering groups
#
#  WHERE filters individual rows BEFORE grouping.
#  HAVING filters groups AFTER aggregation.
#
#  Rule: if you want to filter using COUNT(), SUM(), AVG() etc.
#        you MUST use HAVING — WHERE won't work.
#
#  Java analogy:
#    WHERE  = filter before you group (like an if before a loop)
#    HAVING = filter after you group  (like an if after the loop)
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 4 — HAVING: Filtering Groups               ║")
print("╚══════════════════════════════════════════════════════╝")


# ── WORKED EXAMPLE 4A ───────────────────────────────────
# Find coins that have had more than 2 real trades
# (excluding welcome bonuses — that's what the WHERE does)

run_query(
    "EXAMPLE 4A: Coins with more than 2 real trades",
    """
    SELECT
        c.symbol,
        c.name,
        COUNT(t.trade_id)  AS trade_count,
        SUM(t.amount)      AS total_volume
    FROM   trades t
    JOIN   coins c ON t.coin_id = c.coin_id
    WHERE  t.trade_type != 'welcome'
    GROUP BY c.coin_id
    HAVING COUNT(t.trade_id) > 2
    ORDER BY trade_count DESC
    """
)


# ── TASK 4B — Active receivers ───────────────────────────
# Write a HAVING query that finds students who have RECEIVED
# more than 2 transfers (not counting welcome bonuses).
# Show: handle, number of transfers received.
# Hint: GROUP BY the recipient (to_user_id), JOIN users for the handle.

run_query(
    "TASK 4B: Students with more than 2 transfers received (YOUR QUERY HERE)",
    """
    SELECT
        ???
    FROM trades t
    JOIN users receiver ON ???
    WHERE t.trade_type != 'welcome'
    GROUP BY ???
    HAVING ???
    ORDER BY ???
    """
)


# ── TASK 4C — High-value coin holders ────────────────────
# Write a HAVING query that finds any user holding MORE than
# 100 units of any coin.
# Show: handle, symbol, their balance.
# Tables: balances, users, coins
# Use HAVING on the balance amount.

run_query(
    "TASK 4C: Big holders (balance > 100) (YOUR QUERY HERE)",
    """
    ???
    """
)


# ═══════════════════════════════════════════════════════════
#  SECTION 5 — Blockchain Queries
#
#  The ledger_blocks table IS the blockchain.
#  Each row is one block. Each block seals one trade.
#  The chain: block N's prev_hash must equal block N-1's this_hash
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 5 — The Blockchain                         ║")
print("╚══════════════════════════════════════════════════════╝")


# ── WORKED EXAMPLE 5A ───────────────────────────────────
# Show the full blockchain for YOUR coin

run_query(
    "EXAMPLE 5A: Your coin's blockchain",
    """
    SELECT
        lb.block_id,
        SUBSTR(lb.prev_hash, 1, 16) || '...'  AS prev_hash,
        SUBSTR(lb.this_hash, 1, 16) || '...'  AS this_hash,
        COALESCE(sender.handle, 'SYSTEM')      AS from_user,
        receiver.handle                         AS to_user,
        t.amount,
        t.trade_type
    FROM   ledger_blocks lb
    JOIN   trades t     ON lb.trade_id = t.trade_id
    JOIN   coins  c     ON lb.coin_id  = c.coin_id
    LEFT JOIN users sender   ON t.from_user_id = sender.user_id
    JOIN      users receiver ON t.to_user_id   = receiver.user_id
    WHERE  c.symbol = ?
    ORDER BY lb.block_id ASC
    """,
    (MY_COIN,)
)


# ── TASK 5B — YOUR QUERY: Chain for CLASSUSD ────────────
# Copy the query from 5A but change it to show the CLASSUSD
# blockchain instead of your coin.
# Change the WHERE clause — use 'CLASSUSD' directly (no ?).

run_query(
    "TASK 5B: CLASSUSD blockchain (YOUR QUERY HERE)",
    """
    ???
    """
)


# ── TASK 5C — Find broken chain links ────────────────────
# This is the hardest query in the lab.
# We want to find any block where prev_hash does NOT match
# the previous block's this_hash.
#
# We do this with a self-JOIN — joining ledger_blocks to itself:
#   lb  = the current block
#   lb2 = the previous block (block_id is one less)
#
# Fill in the WHERE clause to find mismatches.

run_query(
    "TASK 5C: Find broken chain links (YOUR QUERY HERE)",
    """
    SELECT
        lb.block_id         AS broken_block,
        lb.prev_hash        AS stored_prev,
        lb2.this_hash       AS actual_prev
    FROM   ledger_blocks lb
    JOIN   ledger_blocks lb2 ON lb2.block_id = lb.block_id - 1
    WHERE  ???    -- hint: compare lb.prev_hash to lb2.this_hash
    """
)

# If this returns 0 rows — great! The chain is intact.
# If it returns rows — those blocks are broken (tampered with).


# ═══════════════════════════════════════════════════════════
#  SECTION 6 — Your Coin's Story
#
#  These queries are completely open — no template.
#  Write SQL that tells the story of YOUR coin using real data.
# ═══════════════════════════════════════════════════════════

print("\n\n╔══════════════════════════════════════════════════════╗")
print("║  SECTION 6 — Your Coin's Story (No Template)        ║")
print("╚══════════════════════════════════════════════════════╝")


# ── TASK 6A — Prove your token rules work ────────────────
# Write a query that provides EVIDENCE of one of your token
# rules working. Examples:
#   Burn:     show trades where burned_amount > 0 for your coin
#   Airdrop:  show trades where trade_type = 'airdrop'
#   Staking:  show trades where trade_type = 'stake_reward'
#   Anti-whale: show that no holder exceeds your max_holding

run_query(
    "TASK 6A: Evidence that my token rule works",
    """
    -- Write your own query here
    -- It should use data from YOUR coin (filter by MY_COIN)
    -- and show proof that your chosen rule is working
    ???
    """
)


# ── TASK 6B — Something interesting ──────────────────────
# Write ONE more query that shows something interesting
# about the class market. It must use at least:
#   - a JOIN
#   - a GROUP BY or HAVING
# Your choice what to investigate.

run_query(
    "TASK 6B: Something interesting I discovered",
    """
    -- Your query here
    ???
    """
)


# ── CLOSE CONNECTION ──────────────────────────────────────
conn.close()
print("\n" + "=" * 55)
print("  Lab complete! Screenshot each section's output.")
print("=" * 55)
