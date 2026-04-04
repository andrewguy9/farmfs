import pytest
from farmfs.api import get_app
from .conftest import build_checksum, build_blob


@pytest.fixture
def app(vol):
    app = get_app({"<root>": vol})
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_api_blob_list_returns(client):
    # Fresh vol has keydb blobs; verify response shape.
    response = client.get("/bs")
    assert response.status_code == 200
    assert isinstance(response.json["blobs"], list)
    assert response.json["next"] is None


def test_api_blob_list_full(vol, client):
    bloba = build_blob(vol, b"a")
    blobb = build_blob(vol, b"b")
    blobc = build_blob(vol, b"c")
    response = client.get("/bs")
    assert response.status_code == 200
    assert bloba in response.json["blobs"]
    assert blobb in response.json["blobs"]
    assert blobc in response.json["blobs"]
    assert response.json["next"] is None


def collect_pages(client, max_items):
    """
    Consume all pages of GET /bs with the given page size.
    Asserts paging invariants: each page sorted, no cross-page duplicates,
    next cursor equals the last item on each page (exclusive for the next call).
    Returns the concatenated blob list.
    """
    all_blobs = []
    cursor = None
    while True:
        qs = {"max_items": max_items}
        if cursor is not None:
            qs["start-after"] = cursor
        r = client.get("/bs", query_string=qs)
        assert r.status_code == 200
        page = r.json["blobs"]
        next_cursor = r.json["next"]
        assert page == sorted(page), f"page not sorted: {page}"
        for b in page:
            assert b not in all_blobs, f"duplicate blob across pages: {b}"
        # next cursor must equal the last item on the page (exclusive for next call)
        if next_cursor is not None:
            assert next_cursor == page[-1], "next cursor must be last item on page"
        all_blobs.extend(page)
        cursor = next_cursor
        if cursor is None:
            break
    assert all_blobs == sorted(all_blobs)
    return all_blobs


def test_api_blob_list_paging(vol, client):
    bloba = build_blob(vol, b"a")
    blobb = build_blob(vol, b"b")
    blobc = build_blob(vol, b"c")

    # Ground truth: unpaged (includes keydb blobs).
    all_blobs = client.get("/bs").json["blobs"]
    n = len(all_blobs)

    # Page size one less than total forces at least two pages.
    page_size = max(1, n - 1)
    collected = collect_pages(client, page_size)
    assert collected == all_blobs
    assert bloba in collected
    assert blobb in collected
    assert blobc in collected


def test_api_blob_list_paging_exact_fit(vol, client):
    build_blob(vol, b"a")
    build_blob(vol, b"b")

    all_blobs = client.get("/bs").json["blobs"]

    # Page size == total: all blobs returned, but since len(page) == max_items
    # we can't know there's no next page without another round-trip.
    r = client.get("/bs", query_string={"max_items": len(all_blobs)})
    assert r.status_code == 200
    assert r.json["blobs"] == all_blobs
    cursor = r.json["next"]
    assert cursor == all_blobs[-1]

    # Follow-up with start-after the last blob returns empty with null next.
    r2 = client.get("/bs", query_string={"start-after": cursor})
    assert r2.status_code == 200
    assert r2.json["blobs"] == []
    assert r2.json["next"] is None


def test_api_blob_list_start_after(vol, client):
    build_blob(vol, b"a")
    build_blob(vol, b"b")
    build_blob(vol, b"c")

    all_blobs = client.get("/bs").json["blobs"]
    # start-after all_blobs[1] should return everything strictly after it
    cursor = all_blobs[1]

    r = client.get("/bs", query_string={"start-after": cursor})
    assert r.status_code == 200
    assert r.json["blobs"] == all_blobs[2:]
    assert r.json["next"] is None


def test_api_blob_create_no_id(vol, client):
    response = client.post("/bs", data=b"a")
    assert response.status_code == 400


def test_api_blob_create(vol, client):
    csuma = build_checksum(b"a")
    response = client.post("/bs", data=b"a", query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers["Location"] == f"http://localhost/bs/{csuma}"
    assert {"duplicate": False, "blob": csuma} == response.json


def test_api_blob_create_dup(vol, client):
    csuma = build_blob(vol, b"a")
    response = client.post("/bs", data=b"a", query_string={"blob": csuma})
    assert response.status_code == 200
    assert response.headers["Location"] == f"http://localhost/bs/{csuma}"
    assert {"duplicate": True, "blob": csuma} == response.json


@pytest.mark.skip("Today the API doesn't validate blob ids")
def test_api_blob_create_invalid_id(vol, client):
    csuma = "abc123"
    response = client.post("/bs", data=b"a", query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers["Location"] == f"http://localhost/bs/{csuma}"
    assert {"duplicate": False, "blob": csuma} == response.json


@pytest.mark.skip("Today the API doesn't validate checksums")
def test_api_blob_create_corrupt_blob(vol, client):
    # TODO the API should validate contents match blob id.
    csuma = build_checksum(b"a")
    response = client.post("/bs", data=b"b", query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers["Location"] == f"http://localhost/bs/{csuma}"
    assert {"duplicate": False, "blob": csuma} == response.json


def test_api_blob_exists_missing(client, vol):
    blob = build_checksum(b"missing")
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 404


def test_api_blob_exists_present(client, vol):
    blob = build_blob(vol, b"a")
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 200


def test_api_blob_delete_missing(client):
    blob = build_checksum(b"missing")
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204


def test_api_blob_delete(vol, client):
    blob = build_blob(vol, b"a")
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204
    # Delete twice, gives same result.
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204
    # Exists check should fail.
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 404


def test_api_blob_missing(vol, client):
    blob = build_checksum(b"missing")
    response = client.get(f"/bs/{blob}")
    assert response.status_code == 404


def test_api_blob_read(vol, client):
    blob = build_blob(vol, b"a")
    response = client.get(f"/bs/{blob}")
    assert response.status_code == 200
    assert response.data == b"a"


def test_api_blob_checksum(vol, client):
    blob = build_blob(vol, b"a")
    csuma = build_checksum(b"a")
    response = client.get(f"/bs/{blob}/checksum")
    assert response.status_code == 200
    assert response.json == {"csum": csuma}
