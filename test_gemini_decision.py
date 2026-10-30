import tempfile
import sqlite3

from decisions import decide_batch


class SQLiteCache:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_cache (
                cache_key TEXT PRIMARY KEY,
                decision TEXT NOT NULL
            )
        """)

    def get_cached(self, key):
        row = self.conn.execute(
            "SELECT decision FROM decision_cache WHERE cache_key = ?",
            (key,)
        ).fetchone()

        if row:
            import json
            return json.loads(row[0])

        return None

    def put_cached(self, key, decision):
        import json
        self.conn.execute(
            "INSERT OR REPLACE INTO decision_cache VALUES (?, ?)",
            (key, json.dumps(decision))
        )
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


package = {
    "packageId": "PKG-TEST-001",
    "documents": [
        {
            "text": """
Invoice INV-TEST-101 from ABC Supplies for 1250.00 INR is valid
and reconciled with the purchase order and confirmed goods receipt.
The amount is within the autonomous payment limit.
[INV-REF-01] [PO-REF-02] [GRN-REF-03]
"""
        },
        {
            "text": """
Policy: Invoices below INR 5000 can be settled autonomously
when fully reconciled.
"""
        }
    ]
}


def main():
    with tempfile.TemporaryDirectory() as tmp:
        with SQLiteCache(f"{tmp}/test.db") as cache:
            decisions = decide_batch(
                packages=[package],
                policy="policy-test-v1",
                cache=cache
            )

            print("\nAI DECISION:")
            for decision in decisions:
                print(decision)


if __name__ == "__main__":
    main()