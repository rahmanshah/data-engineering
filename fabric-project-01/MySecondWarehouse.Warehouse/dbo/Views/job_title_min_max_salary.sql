-- Auto Generated (Do not modify) 3D4C1A329ACB5059A407FEC39700283915077AA9CBC39E4C3267899A63510B93
CREATE VIEW [dbo].[job_title_min_max_salary] AS (SELECT MAX(s.salary) AS max_salary,MIN(s.salary) AS min_salary, t.title
FROM salaries AS s
INNER JOIN titles AS t
ON s.emp_no= t.emp_no
GROUP BY t.title)