import pytest
import requests
import os
from pathlib import Path

BASE_URL = "http://127.0.0.1:8080"
TEST_KEY = "test"
TEST_FILE_PATH = Path("./test_files/img.png")
TEST_FILE_PATH_2 = Path("./test_files/img1.png")
RETRIEVED_FILE_PATH = Path("./retrieved_img.png")

def upload_file(file_path, key):
    url = f"{BASE_URL}/upload/{key}"
    with open(file_path, 'rb') as file:
        response = requests.post(url, files={'file': file})
    return response

def get_file(key, output_path):
    url = f"{BASE_URL}/get/{key}"
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            file.write(response.content)
    return response

def update_file(file_path, key):
    url = f"{BASE_URL}/update/{key}"
    with open(file_path, 'rb') as file:
        response = requests.put(url, files={'file': file})
    return response

def delete_file(key):
    url = f"{BASE_URL}/delete/{key}"
    response = requests.delete(url)
    return response

@pytest.fixture(scope="module")
def cleanup():
    yield
    # Clean up any files created during tests
    if RETRIEVED_FILE_PATH.exists():
        RETRIEVED_FILE_PATH.unlink()

def test_upload_success(cleanup):
    response = upload_file(TEST_FILE_PATH, TEST_KEY)
    assert response.status_code == 200
    assert "File uploaded successfully" in response.text

def test_upload_duplicate_key():
    response = upload_file(TEST_FILE_PATH, TEST_KEY)
    assert response.status_code == 400
    assert "Key already exists" in response.text

def test_get_file_success():
    response = get_file(TEST_KEY, RETRIEVED_FILE_PATH)
    assert response.status_code == 200
    assert RETRIEVED_FILE_PATH.exists()
    assert RETRIEVED_FILE_PATH.stat().st_size > 0

def test_get_file_nonexistent_key():
    response = get_file("abc", RETRIEVED_FILE_PATH)
    assert response.status_code == 404
    assert "Key not found" in response.text

def test_update_file_success():
    response = update_file(TEST_FILE_PATH_2, TEST_KEY)
    assert response.status_code == 200
    assert "File updated successfully" in response.text

def test_update_file_nonexistent_key():
    response = update_file(TEST_FILE_PATH_2, "nonexistent_key")
    assert response.status_code == 404
    assert "Key not found" in response.text

def test_delete_file_success():
    response = delete_file(TEST_KEY)
    assert response.status_code == 200
    assert "File deleted successfully" in response.text

def test_delete_file_nonexistent_key():
    response = delete_file("nonexistent_key")
    assert response.status_code == 404
    assert "Key not found" in response.text

if __name__ == "__main__":
    pytest.main([__file__])