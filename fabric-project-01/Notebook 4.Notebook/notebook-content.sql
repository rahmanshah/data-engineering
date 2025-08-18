-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "d315bed3-3e8d-a1ff-4d4f-c6f66823a7ae",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "d315bed3-3e8d-a1ff-4d4f-c6f66823a7ae",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT MAX(salary) AS max_salary, MIN(salary) AS min_salary
FROM [MySecondWarehouse].[dbo].[salaries]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
