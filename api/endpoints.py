from fastapi import APIRouter, UploadFile, File, HTTPException
from api.services import process_csv_upload, run_query
from api.etl_trigger import run_etl_job

router = APIRouter()

@router.post(
    "/upload_csv/{table_name}",
    summary="Upload CSV file"
)
async def upload_csv(table_name: str, file: UploadFile = File(...)):
    """
    Uploads a CSV file to the server for a specific table.
    Args:
        table_name (str): Target table name ('departments', 'jobs', or 'hired_employees').
        file (UploadFile): The CSV file to upload.
    Returns:
        dict: Confirmation message.
    """
    if table_name not in ["departments", "jobs", "hired_employees"]:
        raise HTTPException(status_code=400, detail="Invalid table name")

    process_csv_upload(file, table_name)
    return {"message": f"File for '{table_name}' uploaded successfully"}

@router.post(
    "/run_etl",
    summary="Trigger ETL process"
)
def run_etl():
    """
    Triggers the ETL process via Spark.
    Returns:
        dict: Status message, details of inserted records, batch ID, and log filename.
    """
    result = run_etl_job()
    if result["status"] == "FAILED":
        raise HTTPException(
            status_code=422,
            detail={"message": "ETL process failed", "errors": result["errors"]}
        )

    inserted = result.get("inserted", {})
    messages = []

    if inserted.get("departments"):
        messages.append(f"{inserted['departments']} records inserted into 'departments'")
    if inserted.get("jobs"):
        messages.append(f"{inserted['jobs']} records inserted into 'jobs'")
    if inserted.get("hired_employees"):
        messages.append(f"{inserted['hired_employees']} records inserted into 'hired_employees'")

    if not messages:
        messages.append("No new records inserted.")

    return {
        "message": "ETL executed successfully.",
        "details": messages,
        "batch_id": result.get("batch_id"),
        "log_file": result.get("log_file")
    }

@router.get(
    "/metrics/hires_per_quarter",
    summary="Hires per quarter by department and job"
)
def hires_per_quarter():
    """
    Retrieves number of hires per quarter grouped by department and job.
    Returns:
        dict: Success message and metric data.
    """
    try:
        data = run_query("hires_per_quarter")
        return {
            "message": "Hires per quarter retrieved successfully",
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get(
    "/metrics/departments_above_avg",
    summary="Departments with hires above average"
)
def departments_above_avg():
    """
    Retrieves departments that hired more than the average in 2021.
    Returns:
        dict: Success message and metric data.
    """
    try:
        data = run_query("departments_above_avg")
        return {
            "message": "Departments with hires above average retrieved successfully",
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))