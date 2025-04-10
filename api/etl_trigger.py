import subprocess
import os
import json
from datetime import datetime

def run_etl_job():
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = "."

        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"etl_outputs/etl_result_{batch_id}.json"

        result = subprocess.run(
            [
                "cmd.exe", "/c", "spark-submit",
                "--jars", "mysql-connector-java-8.0.33.jar",
                "--conf", f"spark.batchId={batch_id}",
                "spark_jobs/validate_and_load.py"
            ],
            capture_output=True,
            text=True,
            check=True,
            env=env
        )

        if not os.path.exists(output_file):
            raise FileNotFoundError(f"ETL output file not found: {output_file}")

        with open(output_file, "r") as f:
            etl_result = json.load(f)

        etl_result["batch_id"] = batch_id
        etl_result["log_file"] = f"etl_{batch_id}.log"

        return etl_result

    except subprocess.CalledProcessError as e:
        return {
            "status": "FAILED",
            "errors": ["ETL execution failed", e.stderr.strip()]
        }
    except Exception as e:
        return {
            "status": "FAILED",
            "errors": [str(e)]
        }