import copy
import uuid

import test_smoke as smoke


def new_batch():
    return smoke.batch(f"negative-{uuid.uuid4().hex}")


def create_task():
    response = smoke.client.post(
        f"{smoke.P}/message:send",
        json=new_batch(),
        headers=smoke.H,
    )
    assert response.status_code == 200
    task = response.json()["task"]
    proposal = task["artifacts"][0]["parts"][0]["data"]["proposals"][0]
    return task, proposal


def result_message(task, proposal, **changes):
    result = {
        "packageId": proposal["packageId"],
        "actionId": proposal["actionId"],
        "action": proposal["action"],
        "outcome": "ACCEPTED",
        "receiptNonce": "n" * 16,
    }
    result.update(changes)
    return {
        "message": {
            "messageId": f"result-{uuid.uuid4().hex}",
            "taskId": task["id"],
            "contextId": task["contextId"],
            "role": "ROLE_USER",
            "parts": [
                {
                    "mediaType": smoke.RESULTS_MT,
                    "data": {"batchId": "b1", "results": [result]},
                }
            ],
        }
    }


def test_request_guards_reject_invalid_inputs():
    url = f"{smoke.P}/message:send"

    assert smoke.client.post(url, data="{", headers=smoke.H).status_code == 400

    wrong_type = {**smoke.H, "Content-Type": "application/json"}
    assert smoke.client.post(url, json=new_batch(), headers=wrong_type).status_code == 415

    invalid_envelope = {"message": {"messageId": "invalid", "parts": []}}
    assert smoke.client.post(url, json=invalid_envelope, headers=smoke.H).status_code == 400

    oversized = {**smoke.H, "Content-Length": str(smoke.main.MAX_BODY + 1)}
    oversized_body = "x" * (smoke.main.MAX_BODY + 1)
    assert smoke.client.post(url, data=oversized_body, headers=oversized).status_code == 413


def test_result_binding_and_validation_errors():
    task, proposal = create_task()
    url = f"{smoke.P}/message:send"

    wrong_context = result_message(task, proposal)
    wrong_context["message"]["contextId"] = "wrong-context"
    assert smoke.client.post(url, json=wrong_context, headers=smoke.H).status_code == 400

    wrong_batch = result_message(task, proposal)
    wrong_batch["message"]["parts"][0]["data"]["batchId"] = "wrong-batch"
    assert smoke.client.post(url, json=wrong_batch, headers=smoke.H).status_code == 400

    assert smoke.client.post(
        url,
        json=result_message(task, proposal, packageId="wrong-package"),
        headers=smoke.H,
    ).status_code == 400

    assert smoke.client.post(
        url,
        json=result_message(task, proposal, action="wrong-action"),
        headers=smoke.H,
    ).status_code == 400

    assert smoke.client.post(
        url,
        json=result_message(task, proposal, outcome="UNKNOWN"),
        headers=smoke.H,
    ).status_code == 400

    assert smoke.client.post(
        url,
        json=result_message(task, proposal, receiptNonce="short"),
        headers=smoke.H,
    ).status_code == 400

    duplicate = result_message(task, proposal)
    duplicate["message"]["parts"][0]["data"]["results"].append(
        copy.deepcopy(duplicate["message"]["parts"][0]["data"]["results"][0])
    )
    assert smoke.client.post(url, json=duplicate, headers=smoke.H).status_code == 400


def test_rejected_result_completes_without_execution_receipt():
    task, proposal = create_task()
    body = result_message(task, proposal, outcome="REJECTED")

    response = smoke.client.post(
        f"{smoke.P}/message:send",
        json=body,
        headers=smoke.H,
    )

    assert response.status_code == 200
    completed = response.json()["task"]
    assert completed["status"]["state"] == "TASK_STATE_COMPLETED"
    assert len(completed["artifacts"]) == 2
    receipts = completed["artifacts"][1]["parts"][0]["data"]["executions"]
    assert receipts == []
