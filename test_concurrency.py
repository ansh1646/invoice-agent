from concurrent.futures import ThreadPoolExecutor
import uuid

import test_api_negative as negative
import test_smoke as smoke


def test_cancel_and_accept_race_has_one_winner():
    task, proposal = negative.create_task()
    result_body = negative.result_message(task, proposal)
    cancel_url = f"{smoke.P}/tasks/{task['id']}:cancel"
    send_url = f"{smoke.P}/message:send"

    def cancel():
        return smoke.client.post(cancel_url, headers=smoke.H)

    def accept():
        result_body["message"]["messageId"] = f"race-{uuid.uuid4().hex}"
        return smoke.client.post(send_url, json=result_body, headers=smoke.H)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda fn: fn(), (cancel, accept)))

    statuses = sorted(response.status_code for response in responses)
    assert statuses == [200, 409]

    final = smoke.client.get(
        f"{smoke.P}/tasks/{task['id']}",
        headers=smoke.H,
    )
    assert final.status_code == 200
    state = final.json()["status"]["state"]
    assert state in {"TASK_STATE_CANCELED", "TASK_STATE_COMPLETED"}

    if state == "TASK_STATE_COMPLETED":
        assert len(final.json()["artifacts"]) == 2
