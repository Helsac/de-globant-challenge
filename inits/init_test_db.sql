DROP DATABASE IF EXISTS test_db;
CREATE DATABASE IF NOT EXISTS test_db;
CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'test_pass';
GRANT ALL PRIVILEGES ON test_db.* TO 'test_user'@'%';
FLUSH PRIVILEGES;
USE test_db;

CREATE TABLE IF NOT EXISTS batch_control (
    batch_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS departments (
    id INT PRIMARY KEY,
    department VARCHAR(255),
    batch_id VARCHAR(255),
    FOREIGN KEY (batch_id) REFERENCES batch_control(batch_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jobs (
    id INT PRIMARY KEY,
    job VARCHAR(255),
    batch_id VARCHAR(255),
    FOREIGN KEY (batch_id) REFERENCES batch_control(batch_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hired_employees (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    datetime DATETIME,
    department_id INT,
    job_id INT,
    batch_id VARCHAR(255),
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (batch_id) REFERENCES batch_control(batch_id) ON DELETE CASCADE
);