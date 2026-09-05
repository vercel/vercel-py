import httpx2 as httpx

import vendor.respx as respx


@respx.mock
def test_httpx2_response_side_effect_sequence() -> None:
    route = respx.get("https://example.test/").mock(
        side_effect=[httpx.Response(200), httpx.Response(201)]
    )

    assert httpx.get("https://example.test/").status_code == 200
    assert httpx.get("https://example.test/").status_code == 201
    assert route.call_count == 2
