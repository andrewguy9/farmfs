import pytest
from farmfs.api import get_app

@pytest.fixture
def app(vol):
    app = get_app({'<root>': vol})
    return app

@pytest.fixture
def client(app):
    return app.test_client()

def test_blob_list(client):
    response = client.get('/bs')
    assert response.status_code == 200
    assert [] == response.json
