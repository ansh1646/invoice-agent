import json
import os
import pathlib

os.environ["PUBLIC_BASE_URL"] = "https://test.example.com/a2a"
os.environ["DB_PATH"] = "test_agent.db"
pathlib.Path("test_agent.db").unlink(missing_ok=True)

from fastapi.testclient import TestClient  # noqa: E402
import decisions                            # noqa: E402
import main                                 # noqa: E402

BATCH_MT, RESULTS_MT = main.BATCH_MT, main.RESULTS_MT
P = main.PREFIX


def fake_llm(system, user):
    out = []
    for line in user.splitlines():
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict) and "packageId" in obj:
            refs = decisions.REF_RE.findall(line)[:3]
            out.append({
                "packageId": obj["packageId"],
                "action": "open_exception",
                "facts": {"vendorName": "Test Vendor", "invoiceNumber": "INV-1",
                          "amountMinor": 125000, "currency": "INR"},
                "evidenceRefs": refs,
                "rationale": f"Propose open_exception because records conflict and need review. Evidence: {', '.join(refs)}.",
            })
    return json.dumps({"decisions": out})


decisions.call_llm = fake_llm
client = TestClient(main.app)
H = {"Authorization": "Bearer alice-token-123456", "A2A-Version": "1.0",
     "Content-Type": "application/a2a+json"}
OTHER = {**H, "Authorization": "Bearer bob-token-999999"}

PKG = {"packageId": "pkg-1", "documents": [
    {"text": "Invoice INV-1 from Test Vendor for 1250.00 INR [INV-REF-01]. "
             "Purchase order matches [PO-REF-02]. Receipt confirmed [GRN-REF-03]."}]}


def batch(mid, cfg=None):
    body = {"message": {"messageId": mid, "role": "ROLE_USER", "parts": [
        {"mediaType": BATCH_MT, "data": {"batchId": "b1", "policyRevision": "p1",
                                         "packages": [PKG]}}]}}
    if cfg is not None:
        body["configuration"] = cfg
    return body


def test_card_and_auth():
    assert client.get("/.well-known/agent-card.json").status_code == 200
    assert client.post(f"{P}/message:send", json=batch("x")).status_code == 401
    bad = {**H, "A2A-Version": "0.3"}
    assert client.post(f"{P}/message:send", json=batch("x"), headers=bad).status_code == 400


def test_full_lifecycle_idempotency_and_isolation():
    r = client.post(f"{P}/message:send", json=batch("m1"), headers=H)
    task = r.json()["task"]
    assert task["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
    prop = task["artifacts"][0]["parts"][0]["data"]["proposals"][0]

    # Replay with reordered config must return the same task, not a new one.
    again = client.post(f"{P}/message:send", json=batch("m1", {"returnImmediately": True}), headers=H)
    assert again.json()["task"]["id"] == task["id"]

    # Same messageId, different content -> conflict.
    changed = batch("m1")
    changed["message"]["parts"][0]["data"]["batchId"] = "b2"
    assert client.post(f"{P}/message:send", json=changed, headers=H).status_code == 409

    cont = {"message": {"messageId": "m2", "taskId": task["id"], "contextId": task["contextId"],
                        "role": "ROLE_USER", "parts": [{"mediaType": RESULTS_MT, "data": {
                            "batchId": "b1", "results": [{
                                "packageId": prop["packageId"], "actionId": prop["actionId"],
                                "action": prop["action"], "outcome": "ACCEPTED",
                                "receiptNonce": "n" * 16}]}}]}}
    done = client.post(f"{P}/message:send", json=cont, headers=H).json()["task"]
    assert done["status"]["state"] == "TASK_STATE_COMPLETED"
    assert len(done["artifacts"]) == 2

    assert client.post(f"{P}/tasks/{task['id']}:cancel", headers=H).status_code == 409
    assert client.get(f"{P}/tasks/{task['id']}", headers=OTHER).status_code == 404
    assert client.get(f"{P}/tasks", headers=OTHER).json() == {"tasks": []}