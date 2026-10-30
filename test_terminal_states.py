import uuid

import test_api_negative as negative
import test_smoke as smoke


def test_canceled_task_rejects_results_and_repeated_cancel():
    task, proposal = negative.create_task()
    cancel_url = f"{smoke.P}/tasks/{task['id']}:cancel"

    canceled = smoke.client.post(cancel_url, headers=smoke.H)
    assert canceled.status_code == 200
    assert canceled.json()["status"]["state"] == "TASK_STATE_CANCELED"

    assert smoke.client.post(cancel_url, headers=smoke.H).status_code == 409

    result = negative.result_message(task, proposal)
    result["message"]["messageId"] = f"after-cancel-{uuid.uuid4().hex}"
    response = smoke.client.post(
        f"{smoke.P}/message:send",
        json=result,
        headers=smoke.H,
    )
    assert response.status_code == 409


def test_completed_task_rejects_a_second_result():
    task, proposal = negative.create_task()
    result = negative.result_message(task, proposal)
    first = smoke.client.post(
        f"{smoke.P}/message:send",
        json=result,
        headers=smoke.H,
    )
    assert first.status_code == 200

    second = negative.result_message(task, proposal)
    second["message"]["messageId"] = f"second-{uuid.uuid4().hex}"
    response = smoke.client.post(
        f"{smoke.P}/message:send",
        json=second,
        headers=smoke.H,
    )
    assert response.status_code == 409


def test_database_errors_use_stable_public_error(monkeypatch):
    def fail(*_args, **_kwargs):
        raise smoke.main.psycopg.OperationalError("secret connection details")

    monkeypatch.setattr(smoke.main.store, "all", fail)
    response = smoke.client.get(f"{smoke.P}/tasks", headers=smoke.H)

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "DATABASE_UNAVAILABLE"}}
    assert "secret" not in response.text
