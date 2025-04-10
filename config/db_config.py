import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": "globant_user",
    "password": "globant_pass",
    "database": "globant_db"
}

TEST_DB_CONFIG = {
    "host": os.getenv("DB_HOST_TEST", "mysql_test"),
    "port": int(os.getenv("DB_PORT_TEST", 3306)),
    "user": "test_user",
    "password": "test_pass",
    "database": "test_db"
}

def get_jdbc_url(test=False):
    cfg = TEST_DB_CONFIG if test else DB_CONFIG
    return f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC"

def get_native_connection(test=False):
    import mysql.connector
    cfg = TEST_DB_CONFIG if test else DB_CONFIG
    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"]
    )