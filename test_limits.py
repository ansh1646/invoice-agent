import asyncio

import main


def test_oversized_a2a_response_is_rejected():
    response = main.a2a({"payload": "x" * main.MAX_RESPONSE})

    assert response.status_code == 413
    assert b"RESPONSE_TOO_LARGE" in response.body


def test_request_timeout_is_explicit():
    assert main.REQUEST_TIMEOUT_SECONDS == 45
    assert main.OVERALL_EXECUTION_SECONDS == 160


def test_timeout_error_is_translated_to_api_error(monkeypatch):
    async def timeout(*_args, **_kwargs):
        raise asyncio.TimeoutError

    async def body(_self):
        return b"{}"

    monkeypatch.setattr(main, "run_in_threadpool", timeout)
    request = type(
        "Request",
        (),
        {
            "headers": {
                "authorization": "Bearer test",
                "A2A-Version": "1.0",
                "content-type": main.A2A_MT,
            },
            "body": body,
        },
    )()

    async def run():
        try:
            await main.message_send(request)
        except main.ApiError as exc:
            return exc.status, exc.code
        raise AssertionError("request did not time out")

    assert asyncio.run(run()) == (504, "REQUEST_TIMEOUT")
