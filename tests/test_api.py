import pytest
from farmfs.api import get_app
from .conftest import build_checksum, build_blob

@pytest.fixture
def app(vol):
    app = get_app({'<root>': vol})
    return app

@pytest.fixture
def client(app):
    return app.test_client()

def test_api_blob_list_empty(client):
    # Empty Depot
    response = client.get('/bs')
    assert response.status_code == 200
    assert [] == response.json

def test_api_blob_list_full(vol, client):
    bloba = build_blob(vol, b'a')
    blobb = build_blob(vol, b'b')
    blobc = build_blob(vol, b'c')
    # Empty Depot
    response = client.get('/bs')
    assert response.status_code == 200
    assert list(sorted([bloba, blobb, blobc])) == response.json

def test_api_blob_create_no_id(vol, client):
    response = client.post("/bs", data=b'a')
    assert response.status_code == 400

def test_api_blob_create(vol, client):
    csuma = build_checksum(b'a')
    response = client.post("/bs", data=b'a', query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers['Location'] == f'http://localhost/bs/{csuma}'
    assert {'duplicate': False, 'blob': csuma} == response.json

def test_api_blob_create_dup(vol, client):
    csuma = build_blob(vol, b'a')
    response = client.post("/bs", data=b'a', query_string={"blob": csuma})
    assert response.status_code == 200
    assert response.headers['Location'] == f'http://localhost/bs/{csuma}'
    assert {'duplicate': True, 'blob': csuma} == response.json

@pytest.mark.skip("Today the API doesn't validate blob ids")
def test_api_blob_create_invalid_id(vol, client):
    csuma = 'abc123'
    response = client.post("/bs", data=b'a', query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers['Location'] == f'http://localhost/bs/{csuma}'
    assert {'duplicate': False, 'blob': csuma} == response.json

@pytest.mark.skip("Today the API doesn't validate checksums")
def test_api_blob_create_corrupt_blob(vol, client):
    # TODO the API should validate contents match blob id.
    csuma = build_checksum(b'a')
    response = client.post("/bs", data=b'b', query_string={"blob": csuma})
    assert response.status_code == 201
    assert response.headers['Location'] == f'http://localhost/bs/{csuma}'
    assert {'duplicate': False, 'blob': csuma} == response.json

def test_api_blob_exists_missing(client, vol):
    blob = build_checksum(b'missing')
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 404

def test_api_blob_exists_present(client, vol):
    blob = build_blob(vol, b'a')
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 200

def test_api_blob_delete_missing(client):
    blob = build_checksum(b'missing')
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204

def test_api_blob_delete(vol, client):
    blob = build_blob(vol, b'a')
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204
    # Delete twice, gives same result.
    response = client.delete(f"/bs/{blob}")
    assert response.status_code == 204
    # Exists check should fail.
    response = client.head(f"/bs/{blob}")
    assert response.status_code == 404

def test_api_blob_missing(vol, client):
    blob = build_checksum(b'missing')
    response = client.get(f"/bs/{blob}")
    assert response.status_code == 404

def test_api_blob_read(vol, client):
    blob = build_blob(vol, b'a')
    response = client.get(f"/bs/{blob}")
    assert response.status_code == 200
    assert response.data == b'a'
