from cdn_controller.controller import Controller


def test_nested_dns_challenge_is_parsed():
    challenge = Controller._dns_challenge({
        "challenges": [{
            "type": "DNS",
            "dnsChallenge": {
                "name": "_acme-challenge.example.com.",
                "type": "CNAME",
                "value": "validation.example.net.",
            },
        }]
    })
    assert challenge == {
        "type": "CNAME",
        "name": "_acme-challenge.example.com.",
        "value": "validation.example.net.",
    }
