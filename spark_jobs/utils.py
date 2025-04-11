from pyspark.sql import DataFrame

def read_csv_with_schema(spark, path, schema):
    """
    Reads a CSV file using the provided schema.
    Args:
        spark (SparkSession): Spark session.
        path (str): Path to the CSV file.
        schema (StructType): Schema to apply to the CSV.
    Returns:
        DataFrame: Parsed DataFrame with the given schema.
    """
    return spark.read.csv(
        path,
        schema=schema,
        header=False,
        mode="DROPMALFORMED",
        sep=","
    )

def write_to_mysql(df, url, table, properties):
    """
    Writes a DataFrame to a MySQL table via JDBC.
    Args:
        df (DataFrame): DataFrame to write.
        url (str): JDBC connection URL.
        table (str): Table name in the database.
        properties (dict): JDBC properties (user, password, driver).
    """
    df.write.jdbc(url=url, table=table, mode="append", properties=properties)

def is_csv_valid_against_schema(csv_path, schema):
    """
    Checks if all rows in the CSV file match the number of fields in the given schema.

    Args:
        csv_path (str): Path to the CSV file.
        schema (StructType): PySpark schema to validate against.

    Returns:
        bool: True if the CSV is valid, False otherwise.
    """
    expected_num_columns = len(schema.fields)

    with open(csv_path, "r", encoding="utf-8") as f:
        for line in f:
            fields = line.strip().split(",")
            if len(fields) != expected_num_columns:
                return False
    return True