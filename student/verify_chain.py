"""
CLASSUSD — Blockchain Verification Script (v3)
================================================
Run with: python verify_chain.py

In v3, each coin has its OWN independent blockchain.
Every block in ledger_blocks belongs to one coin (coin_id).

THE HASH FUNCTION
------------------
data = f"{prev_hash}|{trade_id}|{from_user_id}|{to_user_id}|{coin_id}|{amount}|{executed_at}"
hash = sha256(data.encode()).hexdigest()

THE CHAIN RULE
--------------
Block N's prev_hash must equal Block N-1's this_hash.
The very first block for each coin has prev_hash = "0" * 64.
If any data changes, the hash breaks — and every block after it too.
"""

import sqlite3
import hashlib

DB_PATH = 'classusd.db'


# ════════════════════════════════════════════════════════════
# PART 1 — Compute expected hash for a block
# ════════════════════════════════════════════════════════════

def compute_hash(prev_hash, trade_id, from_user_id, to_user_id, coin_id, amount, executed_at):
    """
    Reproduce the exact hash the server calculated when this block was created.

    Note: from_user_id can be None (for SYSTEM/BANK transactions).
    Python's f-string will render None as the string 'None' — this must
    match what the server produced. Do NOT convert None to 0 or ''.
    """
    data = f"{prev_hash}|{trade_id}|{from_user_id}|{to_user_id}|{coin_id}|{amount}|{executed_at}"
    return hashlib.sha256(data.encode()).hexdigest()


# ════════════════════════════════════════════════════════════
# PART 2 — Verify one coin's chain
# ════════════════════════════════════════════════════════════

def verify_chain(conn, coin_id, symbol):
    """
    Verifies the full blockchain for a single coin.

    Returns: (is_valid, first_bad_block_id, message)
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            lb.block_id,
            lb.trade_id,
            lb.prev_hash,
            lb.this_hash,
            lb.coin_id,
            t.from_user_id,
            t.to_user_id,
            t.amount,
            t.executed_at
        FROM ledger_blocks lb
        JOIN trades t ON lb.trade_id = t.trade_id
        WHERE lb.coin_id = ?
        ORDER BY lb.block_id ASC
    """, (coin_id,))
    blocks = cur.fetchall()

    if not blocks:
        return True, None, f"  {symbol}: No blocks yet."

    genesis_prev = '0' * 64

    for i, block in enumerate(blocks):
        block_id, trade_id, prev_hash, stored_hash, b_coin_id, \
            from_user_id, to_user_id, amount, executed_at = block

        # Check 1: does prev_hash link to previous block?
        expected_prev = genesis_prev if i == 0 else blocks[i-1][3]
        if prev_hash != expected_prev:
            return False, block_id, (
                f"  {symbol} BROKEN at block {block_id}: "
                f"prev_hash mismatch (chain link severed)"
            )

        # Check 2: does this_hash match recomputed hash?
        expected_hash = compute_hash(
            prev_hash, trade_id, from_user_id, to_user_id,
            b_coin_id, amount, executed_at
        )
        if stored_hash != expected_hash:
            return False, block_id, (
                f"  {symbol} TAMPERED at block {block_id}: "
                f"hash mismatch (data was changed after recording)"
            )

    return True, None, f"  {symbol}: ✅ {len(blocks)} blocks — all valid"


# ════════════════════════════════════════════════════════════
# PART 3 — Pretty-print one coin's chain
# ════════════════════════════════════════════════════════════

def print_chain(conn, coin_id, symbol):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            lb.block_id,
            lb.prev_hash,
            lb.this_hash,
            COALESCE(s.handle, 'SYSTEM') AS from_user,
            r.handle                      AS to_user,
            t.amount,
            t.trade_type,
            t.executed_at
        FROM ledger_blocks lb
        JOIN  trades t ON lb.trade_id  = t.trade_id
        LEFT JOIN users s ON t.from_user_id = s.user_id
        JOIN  users r ON t.to_user_id   = r.user_id
        WHERE lb.coin_id = ?
        ORDER BY lb.block_id ASC
    """, (coin_id,))
    blocks = cur.fetchall()

    print(f"\n  {'─'*60}")
    print(f"  {symbol} Blockchain — {len(blocks)} blocks")
    print(f"  {'─'*60}")
    for b in blocks:
        block_id, prev_hash, this_hash, from_user, to_user, amount, trade_type, executed_at = b
        print(f"\n    Block #{block_id}  [{trade_type}]")
        print(f"      {from_user} → {to_user}  ({amount:.4f} {symbol})")
        print(f"      Time:  {executed_at}")
        print(f"      Prev:  {prev_hash[:40]}…")
        print(f"      Hash:  {this_hash[:40]}…")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)

    # Get all coins
    coins = conn.execute("SELECT coin_id, symbol, name FROM coins ORDER BY coin_id").fetchall()

    if not coins:
        print("No coins in the database yet.")
        conn.close()
        exit()

    print(f"\nVerifying {len(coins)} coin chain(s)…\n")

    all_valid = True
    for coin_id, symbol, name in coins:
        # Optionally print the full chain (comment out if too verbose)
        print_chain(conn, coin_id, symbol)

    print(f"\n{'='*62}")
    print("  VERIFICATION RESULTS")
    print(f"{'='*62}")

    for coin_id, symbol, name in coins:
        is_valid, bad_block, msg = verify_chain(conn, coin_id, symbol)
        print(msg)
        if not is_valid:
            all_valid = False
            print(f"    First bad block ID: {bad_block}")

    print(f"\n{'='*62}")
    if all_valid:
        print("  ✅ All chains are valid. No tampering detected.")
    else:
        print("  ❌ Integrity errors found. See above for details.")
        print("  In a real network, affected nodes would be rejected.")
    print(f"{'='*62}\n")

    conn.close()


# ════════════════════════════════════════════════════════════
# EXTENSION CHALLENGES
# ════════════════════════════════════════════════════════════
"""
1. TAMPER DEMO
   Open classusd.db in "DB Browser for SQLite" (free tool).
   Edit an 'amount' field in the trades table.
   Run verify_chain.py — which coin's chain fails? Which block?
   Why do ALL blocks after it also fail?

2. TOY HASH
   Write a second version of compute_hash() using:
       toy_hash = str(sum(ord(c) for c in data) % 10000)
   Does your chain still verify? Why does the length of the hash matter?

3. MULTI-COIN ANALYSIS
   Write a query that finds which coin has the highest burn_rate
   AND has actually had at least 5 trades. Use HAVING.

4. FORK SIMULATION
   What would happen if two students each downloaded the DB,
   made a trade locally, then both uploaded? 
   How does Bitcoin prevent this? (Research: longest chain rule)

5. MERKLE UPGRADE
   Look up Merkle trees. How would you store a Merkle root
   instead of a single prev_hash? What would change in ledger_blocks?
"""
