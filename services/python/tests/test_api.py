import os
import tempfile
import json
import time
import importlib.util
import subprocess
from pathlib import Path

import pytest

# Ensure a clean temporary DB path before importing the app/db modules
tmp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
os.environ['PYTHON_DB_PATH'] = str(tmp_db.name)

# Load the Flask app module by path (so PYTHON_DB_PATH is used by db on import)
spec = importlib.util.spec_from_file_location("py_app", str(Path(__file__).resolve().parents[2] / "services" / "python" / "app.py"))
py_app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(py_app)
app = getattr(py_app, "app")

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c

def load_json(name):
    p = Path(__file__).resolve().parents[1] / "samples" / name
    return json.loads(p.read_text(encoding='utf-8'))

def test_validate_endpoint_success(client):
    payload = load_json('valid_track.json')
    rv = client.post('/validate', json=payload)
    assert rv.status_code == 200
    data = rv.get_json()
    assert data.get('valid') is True

def test_validate_endpoint_bad_request(client):
    rv = client.post('/validate', json={})
    assert rv.status_code == 400
    data = rv.get_json()
    assert data.get('valid') is False or 'error' in data

def test_save_and_split(client):
    payload = load_json('valid_track.json')
    # Save
    rv = client.post('/save', json=payload)
    assert rv.status_code == 201
    data = rv.get_json()
    assert data.get('saved') is True
    assert 'track_id' in data

    # Query split endpoint
    writers = payload.get('songwriters', [])
    rv2 = client.post('/split', json={'writers': writers})
    assert rv2.status_code == 200
    data2 = rv2.get_json()
    assert data2.get('total') == sum(w.get('split', 0) for w in writers)