import tempfile

from store import Store
from decisions import decide_batch

package = {
    "packageId": "PKG-TEST-001",
    "documents": [
        {
            "text": """
            Invoice INV-TEST-101 from ABC Supplies for
            1250.00 INR is valid and reconciled with the
            purchase order and confirmed goods receipt.
            The amount is within the autonomous payment limit.
            [INV-REF-01] [PO-REF-02] [GRN-REF-03]
            """
        },
        {
            "text": """
            Policy: Invoices below INR 5000 can be
            settled autonomously when fully reconciled.
            """
        }
    ]
}

def main():
    with tempfile.TemporaryDirectory() as tmp:
        with Store(f"{tmp}/test.db") as cache:
            decisions = decide_batch(
                packages=[package],
                policy="policy-test-v1",
                cache=cache
            )

            for decision in decisions:
                print("\nAI DECISION:")
                print(decision)


if __name__ == "__main__":
    main()
        