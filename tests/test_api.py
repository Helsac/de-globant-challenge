import os
import pytest
from fastapi.testclient import TestClient
from main_api import app

UPLOAD_DIR = "data/uploads"

client = TestClient(app)

def setup_module(module):
    os.environ["IS_TEST"] = "True"

    sample_files = {
        "departments": "tests/sample_departments.csv",
        "jobs": "tests/sample_jobs.csv",
        "hired_employees": "tests/sample_hired_employees.csv"
    }

    for table, filepath in sample_files.items():
        with open(filepath, "rb") as f:
            response = client.post(
                f"/upload_csv/{table}",
                files={"file": (f"{table}.csv", f, "text/csv")}
            )
            assert response.status_code == 200, f"Failed upload for {table}"

def teardown_module(module):
    for filename in os.listdir(UPLOAD_DIR):
        if filename.endswith(".csv"):
            os.remove(os.path.join(UPLOAD_DIR, filename))
    os.environ["IS_TEST"] = "False"

def test_upload_csv_departments():
    filepath = "tests/sample_departments.csv"
    with open(filepath, "rb") as file:
        response = client.post(
            "/upload_csv/departments",
            files={"file": ("departments.csv", file, "text/csv")}
        )
    assert response.status_code == 200
    assert "uploaded successfully" in response.json()["message"]

def test_upload_csv_jobs():
    filepath = "tests/sample_jobs.csv"
    with open(filepath, "rb") as file:
        response = client.post(
            "/upload_csv/jobs",
            files={"file": ("jobs.csv", file, "text/csv")}
        )
    assert response.status_code == 200
    assert "uploaded successfully" in response.json()["message"]

def test_upload_csv_hired_employees():
    filepath = "tests/sample_hired_employees.csv"
    with open(filepath, "rb") as file:
        response = client.post(
            "/upload_csv/hired_employees",
            files={"file": ("hired_employees.csv", file, "text/csv")}
        )
    assert response.status_code == 200
    assert "uploaded successfully" in response.json()["message"]

def test_run_etl_success():
    response = client.post("/run_etl")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "ETL executed successfully."
    assert "batch_id" in data
    assert "log_file" in data

def test_upload_invalid_table():
    with open("data/test_invalid.csv", "w") as f:
        f.write("id,name\n1,Invalid")
    with open("data/test_invalid.csv", "rb") as f:
        response = client.post(
            "/upload_csv/invalid_table",
            files={"file": ("test_invalid.csv", f, "text/csv")},
        )
    os.remove("data/test_invalid.csv")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid table name"

def test_get_hires_per_quarter():
    response = client.get("/metrics/hires_per_quarter")
    assert response.status_code == 200

def test_get_top_departments():
    response = client.get("/metrics/departments_above_avg")
    assert response.status_code == 200

