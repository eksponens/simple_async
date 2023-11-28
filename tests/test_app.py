from flask import json
from unittest.mock import patch
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
import pytest

from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


def test_hello(client):
    response = client.get('/greeting')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['title'] == 'Welcome aboard, captain! All systems online! To init the database go to /init'


@patch('app.create_async_engine')
@patch('app.get_all_data_raw')
def test_get_all_data(mock_get_all_data, mock_create_async_engine, client):
    mock_result = [(1, "Test 1"), (2, "Test 2"), (3, "Test 3")]
    mock_get_all_data.return_value = mock_result

    response = client.get('/')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == len(mock_result)
    assert data[0]['id'] == mock_result[0][0]
    assert data[0]['text'] == mock_result[0][1]


@patch('app.create_async_engine')
@patch('app.get_data_orm')
def test_get_data(mock_get_data_orm, mock_create_async_engine, client):
    mock_result = [(1, "Test 1"), (2, "Test 2"), (3, "Test 3")]
    mock_get_data_orm.return_value = mock_result

    response = client.post('/')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == len(mock_result)
    assert data[0]['id'] == mock_result[0][0]
    assert data[0]['text'] == mock_result[0][1]
    assert data[1]['id'] == mock_result[1][0]
    assert data[1]['text'] == mock_result[1][1]
    assert data[2]['id'] == mock_result[2][0]
    assert data[2]['text'] == mock_result[2][1]

    mock_result = [(2, "Test 2"), (6, "Test 6"), (12, "Test 12")]
    mock_get_data_orm.return_value = mock_result

    response = client.post('/', json={"ids": [2, 12, 85, 6]})
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == len(mock_result)
    assert data[0]['id'] == mock_result[0][0]
    assert data[0]['text'] == mock_result[0][1]
    assert data[1]['id'] == mock_result[1][0]
    assert data[1]['text'] == mock_result[1][1]
    assert data[2]['id'] == mock_result[2][0]
    assert data[2]['text'] == mock_result[2][1]


@patch('app.create_async_engine')
@patch('app.metadata')
@patch('app.datasource1')
@patch('app.datasource2')
@patch('app.datasource3')
def test_init(mock_datasource3, mock_datasource2, mock_datasource1, mock_metadata, mock_create_async_engine, client):
    response = client.post('/init', json={'init': 1})
    assert response.status_code == 200
    assert response.data == b'The database was initialized'

    # Assert that the tables were created and the data was inserted
    mock_create_async_engine.assert_called_once_with('postgresql+asyncpg://postgres:Pa$$w0rd@localhost:5432/postgres')
    assert mock_datasource1.insert.call_count == 20
    assert mock_datasource2.insert.call_count == 20
    assert mock_datasource3.insert.call_count == 20


if __name__ == '__main__':
    pytest.main()
