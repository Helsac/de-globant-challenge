import os
import json
from datetime import datetime
import docker

def run_etl_job():
    try:
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"/opt/spark/app/etl_outputs/etl_result_{batch_id}.json"

        client = docker.from_env()
        container = client.containers.get("spark_container")

        # Comando spark-submit
        cmd = [
            "/usr/local/bin/spark-submit",
            "--jars", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.33.jar",
            "--driver-class-path", "/opt/bitnami/spark/jars/mysql-connector-java-8.0.33.jar",
            "--conf", f"spark.executor.extraClassPath=/opt/bitnami/spark/jars/mysql-connector-java-8.0.33.jar",
            "--conf", f"spark.batchId={batch_id}",
            "spark_jobs/validate_and_load.py"
        ]

        exec_result = container.exec_run(cmd, stdout=True, stderr=True)
        logs = exec_result.output.decode()

        if not os.path.exists(output_file):
            raise FileNotFoundError(f"ETL output file not found: {output_file}")

        with open(output_file, "r") as f:
            etl_result = json.load(f)

        etl_result["batch_id"] = batch_id
        etl_result["log_file"] = f"etl_{batch_id}.log"
        etl_result["logs"] = logs

        return etl_result

    except docker.errors.APIError as e:
        return {
            "status": "FAILED",
            "errors": ["Docker API error", str(e)]
        }
    except Exception as e:
        return {
            "status": "FAILED",
            "errors": [str(e)]
        }