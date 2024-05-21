-- Databricks notebook source
SELECT * FROM default.employeetable_csv

-- COMMAND ----------

SELECT SUM (Salary) FROM default.employeetable_csv

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM departmenttable_csv

-- COMMAND ----------

-- DBTITLE 1,Add filter with WHERE Clause
SELECT * FROM employeetable_csv WHERE Salary>=35000

-- COMMAND ----------

-- DBTITLE 1,SORT Query
SELECT * FROM employeetable_csv ORDER BY Region DESC

-- COMMAND ----------

-- DBTITLE 1,Aggregated-- sum, min, max, average...
SELECT max(Salary) FROM employeetable_csv

-- COMMAND ----------

-- DBTITLE 1,GROUP BY
SELECT Region, sum(Salary) Total_Salary FROM employeetable_csv GROUP BY 1 ORDER BY 2 DESC

-- COMMAND ----------

SELECT * FROM departmenttable_csv

-- COMMAND ----------

-- DBTITLE 1,JOIN
SELECT em.Name, dp.Department, em.Salary FROM employeetable_csv em INNER JOIN departmenttable_csv dp ON em.UniqueID = dp.UniqueID

-- COMMAND ----------

-- DBTITLE 1,JOINB CONTD.
SELECT dp.Department, sum(em.Salary) FROM departmenttable_csv dp LEFT JOIN employeetable_csv em ON em.UniqueID = dp.UniqueID GROUP BY 1

-- COMMAND ----------

-- DBTITLE 1,UNION
SELECT em.UniqueID Unique FROM employeetable_csv em UNION SELECT dp.UniqueID FROM departmenttable_csv dp ORDER BY Unique

-- COMMAND ----------

-- DBTITLE 1,Advance SQL (Common Table Expression-CTE)
WITH Virtual_Table AS (
SELECT * FROM employeetable_csv  
)
SELECT UniqueID, Name, Region FROM Virtual_Table

-- COMMAND ----------

-- DBTITLE 1,WINDOWS Function -- row_number, rank, dense_rank
SELECT *, row_number() OVER (PARTITION BY UniqueID ORDER BY UniqueID) AS rn FROM employeetablecte_csv

-- COMMAND ----------

-- DBTITLE 1,Remove duplicate
WITH VCTE AS (
SELECT *, row_number() OVER (PARTITION BY UniqueID ORDER BY UniqueID) AS rn FROM employeetablecte_csv
)
SELECT  * FROM VCTE WHERE rn=1

-- COMMAND ----------

-- DBTITLE 1,Create a temp view
CREATE OR REPLACE temp view remove_dp as
SELECT *, row_number() OVER (PARTITION BY UniqueID ORDER BY UniqueID) AS rn FROM default.employeetablecte_csv

-- COMMAND ----------

-- DBTITLE 1,Call the temp view
SELECT * FROM remove_dp

-- COMMAND ----------

-- DBTITLE 1,Temp view vs Permanent view
/*To create a Permanent View, change temp view to view
---temp view can only be called in the notebook but permanent view can be called anywhere */

-- COMMAND ----------

-- DBTITLE 1,Create Table from a windows query
CREATE OR REPLACE table remove_dp_table as
SELECT *, row_number() OVER (PARTITION BY UniqueID ORDER BY UniqueID) AS rn FROM default.employeetablecte_csv

-- COMMAND ----------

-- DBTITLE 1,Select table
SELECT * FROM remove_dp_table

-- COMMAND ----------


