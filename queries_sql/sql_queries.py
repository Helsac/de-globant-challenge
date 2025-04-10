SQL_QUERIES = {
    "hires_per_quarter": """
        SELECT
          d.department,
          j.job,
          SUM(CASE WHEN QUARTER(h_emp.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
          SUM(CASE WHEN QUARTER(h_emp.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
          SUM(CASE WHEN QUARTER(h_emp.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
          SUM(CASE WHEN QUARTER(h_emp.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM hired_employees AS h_emp
        JOIN departments AS d ON d.id = h_emp.department_id
        JOIN jobs AS j ON j.id = h_emp.job_id
        WHERE YEAR(h_emp.datetime) = 2021
        GROUP BY d.department, j.job
        ORDER BY d.department, j.job;
    """,

    "departments_above_avg": """
        SELECT
          d.id,
          d.department,
          COUNT(h_emp.id) AS hired
        FROM hired_employees AS h_emp
        JOIN departments AS d ON d.id = h_emp.department_id
        WHERE YEAR(h_emp.datetime) = 2021
        GROUP BY d.id, d.department
        HAVING hired > (
          SELECT AVG(total) FROM (
            SELECT COUNT(*) AS total
            FROM hired_employees
            WHERE YEAR(datetime) = 2021
            GROUP BY department_id
          ) AS dep_avg
        )
        ORDER BY hired DESC;
    """
}