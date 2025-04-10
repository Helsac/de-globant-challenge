from pyspark.sql.types import StructType, StructField, StringType, IntegerType

departments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("department", StringType(), True)
])

jobs_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("job", StringType(), True)
])

hired_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True)
])
