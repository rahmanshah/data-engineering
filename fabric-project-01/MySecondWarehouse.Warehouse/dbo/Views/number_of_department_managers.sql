-- Auto Generated (Do not modify) 47BA3FE50BD1D5F167A1944477E6727D1D6DBC202F3BE366C43510B01BDBBAAE
CREATE VIEW [dbo].[number_of_department_managers] AS (SELECT d.dept_name AS Department, COUNT(m.emp_no) AS NUMBER_of_Manager
FROM departments AS d
INNER JOIN dept_manager AS m
ON d.dept_no = m.dept_no
GROUP BY d.dept_name)