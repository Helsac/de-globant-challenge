from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, col
import uuid
import logging
from logging.handlers import RotatingFileHandler
from config.db_config import get_jdbc_url, get_native_connection
from spark_jobs.utils import read_csv_with_schema, is_csv_valid_against_schema
from spark_jobs.schemas import departments_schema, jobs_schema, hired_schema
import json
import os
from datetime import datetime

try:
    spark = SparkSession.builder.getOrCreate()
    log_id = spark.sparkContext.getConf().get("spark.batchId", datetime.now().strftime("%Y%m%d_%H%M%S"))
    is_test_str = spark.sparkContext.getConf().get("spark.isTest", "False")
    IS_TEST = is_test_str.lower() == "true"
except:
    log_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    IS_TEST = False

os.makedirs("logs", exist_ok=True)
log_filename = f"logs/etl_{log_id}.log"
log_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
file_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
data_path = "/opt/spark/app/data/uploads"
validation_errors = []

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(), file_handler]
)

logging.info(f"Logs will be saved to: {os.path.abspath(log_filename)}")

def create_spark_session():
    """
    Creates and returns a Spark session with predefined configuration.

    Returns:
        SparkSession: Configured Spark session.
    """
    logging.info("Creating Spark session...")
    return SparkSession.builder \
        .appName("Globant ETL") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()


def load_csv_data(spark):
    """
    Loads CSV files from the data directory if they exist and pass schema validation.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        tuple: DataFrames for departments, jobs, and hired employees.
    """
    logging.info("Loading CSV files from data directory...")

    departments_path = os.path.join(data_path, "departments.csv")
    jobs_path = os.path.join(data_path, "jobs.csv")
    hired_path = os.path.join(data_path, "hired_employees.csv")

    departments_csv = spark.createDataFrame([], departments_schema)
    jobs_csv = spark.createDataFrame([], jobs_schema)
    hired_df = spark.createDataFrame([], hired_schema)

    if os.path.exists(departments_path):
        if is_csv_valid_against_schema(departments_path, departments_schema):
            departments_csv = read_csv_with_schema(spark, departments_path, departments_schema)
        else:
            validation_errors.append("departments.csv does not match the expected schema.")
    else:
        logging.warning("departments.csv not found, skipping.")

    if os.path.exists(jobs_path):
        if is_csv_valid_against_schema(jobs_path, jobs_schema):
            jobs_csv = read_csv_with_schema(spark, jobs_path, jobs_schema)
        else:
            validation_errors.append("jobs.csv does not match the expected schema.")
    else:
        logging.warning("jobs.csv not found, skipping.")

    if os.path.exists(hired_path):
        if is_csv_valid_against_schema(hired_path, hired_schema):
            hired_df = read_csv_with_schema(spark, hired_path, hired_schema)
            hired_df = hired_df.withColumn("datetime", to_timestamp("datetime", "yyyy-MM-dd'T'HH:mm:ssX"))
        else:
            validation_errors.append("hired_employees.csv does not match the expected schema.")
    else:
        logging.warning("hired_employees.csv not found, skipping.")

    return departments_csv, jobs_csv, hired_df

def validate_data(spark, departments_csv, jobs_csv, hired_df, jdbc_url, props, batch_id):
    """
    Validates incoming data and returns new entries to be inserted.

    Args:
        spark (SparkSession): Spark session.
        departments_csv (DataFrame): Departments data.
        jobs_csv (DataFrame): Jobs data.
        hired_df (DataFrame): Hired employees data.
        jdbc_url (str): JDBC connection string.
        props (dict): JDBC connection properties.
        batch_id (str): Batch identifier.

    Returns:
        tuple: new_departments, new_jobs, hired_validated, validation_errors
    """
    logging.info("Starting data validation...")
    new_departments = spark.createDataFrame([], departments_csv.schema)
    new_jobs = spark.createDataFrame([], jobs_csv.schema)
    hired_validated = spark.createDataFrame([], hired_df.schema)

    departments_db = spark.read.jdbc(jdbc_url, "departments", properties=props)
    jobs_db = spark.read.jdbc(jdbc_url, "jobs", properties=props)
    existing_ids = spark.read.jdbc(jdbc_url, "hired_employees", properties=props).select("id")

    if departments_csv.count() > 0:
        logging.info("Validating departments.csv...")
        if not {"id", "department"}.issubset(set(departments_csv.columns)):
            validation_errors.append("departments.csv schema mismatch.")
        else:
            if departments_csv.groupBy("id").count().filter("count > 1").count() > 0:
                validation_errors.append("Duplicate IDs in departments.csv")
            if departments_csv.filter(departments_csv.department.isNull()).count() > 0:
                validation_errors.append("Empty department names in departments.csv")
            new_departments = departments_csv.join(departments_db.select("id"), on="id", how="left_anti")
            new_departments = new_departments.withColumn("batch_id", lit(batch_id)).dropDuplicates(["id"])
            if new_departments.count() == 0:
                validation_errors.append("No new departments to insert.")

    if jobs_csv.count() > 0:
        logging.info("Validating jobs.csv...")
        if not {"id", "job"}.issubset(set(jobs_csv.columns)):
            validation_errors.append("jobs.csv schema mismatch.")
        else:
            if jobs_csv.groupBy("id").count().filter("count > 1").count() > 0:
                validation_errors.append("Duplicate IDs in jobs.csv")
            if jobs_csv.filter(jobs_csv.job.isNull()).count() > 0:
                validation_errors.append("Empty job names in jobs.csv")
            new_jobs = jobs_csv.join(jobs_db.select("id"), on="id", how="left_anti")
            new_jobs = new_jobs.withColumn("batch_id", lit(batch_id)).dropDuplicates(["id"])
            if new_jobs.count() == 0:
                validation_errors.append("No new jobs to insert.")

    if hired_df.count() > 0:
        logging.info("Validating hired_employees.csv...")
        if not {"id", "name", "datetime", "department_id", "job_id"}.issubset(set(hired_df.columns)):
            validation_errors.append("hired_employees.csv schema mismatch.")
        else:
            if hired_df.groupBy("id").count().filter("count > 1").count() > 0:
                validation_errors.append("Duplicate IDs in hired_employees.csv")
            hired_validated = hired_df.join(existing_ids, on="id", how="left_anti")
            hired_validated = hired_validated.withColumn("batch_id", lit(batch_id)).dropDuplicates(["id"])
            all_departments = departments_db.select("id").union(new_departments.select("id")).dropDuplicates()
            all_jobs = jobs_db.select("id").union(new_jobs.select("id")).dropDuplicates()
            hired_validated = hired_validated \
                .join(all_departments.withColumnRenamed("id", "valid_dept_id"),
                      hired_validated.department_id == col("valid_dept_id"), "inner") \
                .join(all_jobs.withColumnRenamed("id", "valid_job_id"),
                      hired_validated.job_id == col("valid_job_id"), "inner") \
                .drop("valid_dept_id", "valid_job_id")
            if hired_validated.count() == 0:
                validation_errors.append("No valid hired employees to insert.")

    if validation_errors:
        logging.warning("Validation errors found: %s", validation_errors)
        return None, None, None, validation_errors

    logging.info("Validation completed successfully.")
    return new_departments, new_jobs, hired_validated, []

def update_batch_status(batch_id, status):
    """
    Inserts or updates the status of a batch in the control table.

    Args:
        batch_id (str): Batch identifier.
        status (str): New status (e.g., PENDING, SUCCESS, FAILED).
    """
    logging.info(f"Updating batch status to {status} for batch_id: {batch_id}")
    with get_native_connection(test=IS_TEST) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO batch_control (batch_id, status)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE status=%s
            """, (batch_id, status, status))
            conn.commit()

def rollback_batch(batch_id, error):
    """
    Rolls back data inserted in case of failure.

    Args:
        batch_id (str): Batch identifier.
        error (Exception): The exception that caused the rollback.
    """
    logging.error(f"Rollback initiated for batch {batch_id} due to error: {error}")
    with get_native_connection(test=IS_TEST) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM hired_employees WHERE batch_id = %s", (batch_id,))
            cursor.execute("DELETE FROM jobs WHERE batch_id = %s", (batch_id,))
            cursor.execute("DELETE FROM departments WHERE batch_id = %s", (batch_id,))
            update_batch_status(batch_id, "FAILED")
            conn.commit()

def insert_data(new_departments, new_jobs, hired_validated, jdbc_url, props):
    """
    Writes new data into the corresponding MySQL tables.

    Args:
        new_departments (DataFrame): Validated department data.
        new_jobs (DataFrame): Validated job data.
        hired_validated (DataFrame): Validated hired employees data.
        jdbc_url (str): JDBC connection string.
        props (dict): JDBC connection properties.
    """
    logging.info("Inserting data into the database...")
    inserted = {
        "departments": new_departments.count() if new_departments else 0,
        "jobs": new_jobs.count() if new_jobs else 0,
        "hired_employees": hired_validated.count() if hired_validated else 0
    }

    if inserted["departments"] > 0:
        new_departments.write.jdbc(jdbc_url, "departments", mode="append", properties=props)
    if inserted["jobs"] > 0:
        new_jobs.write.jdbc(jdbc_url, "jobs", mode="append", properties=props)
    if inserted["hired_employees"] > 0:
        hired_validated.write.jdbc(jdbc_url, "hired_employees", mode="append", properties=props)

    logging.info("Data insertion completed.")
    return inserted

def main():
    """
    Main driver function for executing the ETL process.
    It loads, validates, inserts data and manages batch status.
    """
    logging.info("Starting ETL process...")
    spark = create_spark_session()
    if IS_TEST:
        jdbc_url = get_jdbc_url(test=True)
        props = {"user": "test_user", "password": "test_pass", "driver": "com.mysql.cj.jdbc.Driver"}
    else:
        jdbc_url = get_jdbc_url()
        props = {"user": "globant_user", "password": "globant_pass", "driver": "com.mysql.cj.jdbc.Driver"}
    batch_id = log_id
    etl_result = {
        "status": "SUCCESS",
        "errors": []
    }
    try:
        update_batch_status(batch_id, "PENDING")
        departments_csv, jobs_csv, hired_df = load_csv_data(spark)
        new_departments, new_jobs, hired_validated, validation_errors = validate_data(
            spark, departments_csv, jobs_csv, hired_df, jdbc_url, props, batch_id)

        if validation_errors:
            etl_result["status"] = "FAILED"
            etl_result["errors"].extend(validation_errors)
            rollback_batch(batch_id, "Validation errors")
        else:
            inserted_counts = insert_data(new_departments, new_jobs, hired_validated, jdbc_url, props)
            etl_result["inserted"] = inserted_counts
            update_batch_status(batch_id, "SUCCESS")
            logging.info(f"Batch {batch_id} successfully completed.")

    except Exception as e:
        rollback_batch(batch_id, e)
        etl_result["status"] = "FAILED"
        etl_result["errors"].append(str(e))

    os.makedirs("/opt/spark/app/etl_outputs", exist_ok=True)
    output_path = f"/opt/spark/app/etl_outputs/etl_result_{batch_id}.json"
    with open(output_path, "w") as f:
        json.dump(etl_result, f)

    for file_name in ["departments.csv", "jobs.csv", "hired_employees.csv"]:
        try:
            file_path = os.path.join(data_path, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Removed uploaded file: {file_path}")
        except Exception as e:
            logging.warning(f"Could not remove file {file_name}: {e}")

    print(f"ETL finished. Output saved to {output_path}")


if __name__ == "__main__":
    logging.info("Starting ETL via spark-submit...")
    main()