import json

import decisions


PACKAGE = {
    "packageId": "audit-pkg-1",
    "documents": [
        {
            "text": (
                "Invoice INV-AUDIT-001 from Audit Vendor for 1250.00 INR "
                "matches the purchase order and receipt. "
                "[INV-AUDIT-01] [PO-AUDIT-02] [GRN-AUDIT-03]"
            )
        },
        {
            "text": "Policy: invoices below INR 5000 may be settled when reconciled."
        },
    ],
}


def valid_decision(action="settle_invoice"):
    return {
        "packageId": PACKAGE["packageId"],
        "action": action,
        "facts": {
            "vendorName": "Audit Vendor",
            "invoiceNumber": "INV-AUDIT-001",
            "amountMinor": 125000,
            "currency": "INR",
        },
        "evidenceRefs": [
            "[INV-AUDIT-01]",
            "[PO-AUDIT-02]",
            "[GRN-AUDIT-03]",
        ],
        "rationale": (
            f"{action} is supported by the reconciled invoice and the "
            "purchase-order and receipt evidence [INV-AUDIT-01] "
            "[PO-AUDIT-02]."
        ),
    }


class MemoryCache:
    def __init__(self):
        self.values = {}

    def get_cached(self, key):
        return self.values.get(key)

    def put_cached(self, key, value):
        self.values[key] = value


def test_validate_accepts_all_supported_actions():
    for action in decisions.ACTIONS:
        assert decisions.validate(valid_decision(action), PACKAGE) == []


def test_validate_rejects_fabricated_facts_and_decoy_references():
    fabricated = valid_decision()
    fabricated["facts"]["vendorName"] = "Fabricated Vendor"
    fabricated["evidenceRefs"] = ["[INV-AUDIT-01]", "[PO-AUDIT-02]", "[DECOY-99]"]

    errors = decisions.validate(fabricated, PACKAGE)

    assert "vendorName_not_in_documents" in errors
    assert "refs_not_in_package" in errors


def test_validate_requires_references_in_one_paragraph():
    split_package = {
        "packageId": PACKAGE["packageId"],
        "documents": [
            {"text": "Invoice INV-AUDIT-001 from Audit Vendor [INV-AUDIT-01]."},
            {"text": "Purchase order [PO-AUDIT-02] and receipt [GRN-AUDIT-03]."},
        ],
    }

    errors = decisions.validate(valid_decision(), split_package)

    assert "refs_not_in_same_paragraph" in errors


def test_decision_cache_reuses_only_same_policy_and_package(monkeypatch):
    cache = MemoryCache()
    calls = []

    def fake_llm(_system, _user):
        calls.append(True)
        return json.dumps({"decisions": [valid_decision()]})

    monkeypatch.setattr(decisions, "call_llm", fake_llm)

    first = decisions.decide_batch([PACKAGE], "policy-v1", cache)
    second = decisions.decide_batch([PACKAGE], "policy-v1", cache)
    third = decisions.decide_batch([PACKAGE], "policy-v2", cache)

    assert first == second == third
    assert len(calls) == 2


def test_twelve_invoice_scenarios_cover_all_actions(monkeypatch):
    scenarios = [
        ("settle", "settle_invoice"),
        ("approval", "request_approval"),
        ("missing-receipt", "hold_invoice"),
        ("duplicate", "reject_duplicate"),
        ("amount-conflict", "open_exception"),
        ("same-vendor-new-invoice", "settle_invoice"),
        ("missing-invoice-number", "hold_invoice"),
        ("currency-conflict", "open_exception"),
        ("vendor-conflict", "open_exception"),
        ("unclear-evidence", "hold_invoice"),
        ("prompt-injection", "settle_invoice"),
        ("archive-decoy", "settle_invoice"),
    ]
    packages = []
    expected = {}

    for package_id, action in scenarios:
        package = {
            "packageId": package_id,
            "documents": [
                {
                    "text": (
                        f"Invoice INV-{package_id} from Vendor {package_id} "
                        f"for 1250.00 INR supports review. "
                        f"[{package_id}-INV] [{package_id}-PO] [{package_id}-GRN]"
                    )
                }
            ],
        }
        packages.append(package)
        expected[package_id] = action

    def fake_llm(_system, user):
        output = []
        for line in user.splitlines():
            try:
                package = json.loads(line)
            except ValueError:
                continue
            package_id = package.get("packageId")
            if package_id not in expected:
                continue
            output.append(
                {
                    "packageId": package_id,
                    "action": expected[package_id],
                    "facts": {
                        "vendorName": f"Vendor {package_id}",
                        "invoiceNumber": f"INV-{package_id}",
                        "amountMinor": 125000,
                        "currency": "INR",
                    },
                    "evidenceRefs": [
                        f"[{package_id}-INV]",
                        f"[{package_id}-PO]",
                        f"[{package_id}-GRN]",
                    ],
                    "rationale": (
                        f"{expected[package_id]} is supported by the invoice "
                        f"evidence [{package_id}-INV] and purchase-order "
                        f"evidence [{package_id}-PO]."
                    ),
                }
            )
        return json.dumps({"decisions": output})

    monkeypatch.setattr(decisions, "call_llm", fake_llm)
    results = decisions.decide_batch(packages, "policy-audit-v1", MemoryCache())

    assert len(results) == 12
    assert {result["action"] for result in results} == set(decisions.ACTIONS)
    assert {
        result["packageId"]: result["action"] for result in results
    } == expected
