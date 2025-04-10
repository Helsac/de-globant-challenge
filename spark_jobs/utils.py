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