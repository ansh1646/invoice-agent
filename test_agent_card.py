import test_smoke as smoke


def test_agent_card_contract():
    response = smoke.client.get("/.well-known/agent-card.json")

    assert response.status_code == 200
    card = response.json()
    interface = card["supportedInterfaces"][0]

    assert interface == {
        "url": "https://test.example.com/a2a",
        "protocolBinding": "HTTP+JSON",
        "protocolVersion": "1.0",
    }
    assert card["capabilities"] == {
        "streaming": False,
        "pushNotifications": False,
    }
    assert card["defaultInputModes"] == [
        smoke.BATCH_MT,
        smoke.RESULTS_MT,
    ]
    assert card["defaultOutputModes"] == [
        smoke.main.PROPOSALS_MT,
        smoke.main.RECEIPTS_MT,
    ]
    assert card["skills"][0]["id"] == "invoice_action_agent"
