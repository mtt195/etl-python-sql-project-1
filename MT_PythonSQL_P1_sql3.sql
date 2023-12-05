/* -------------------------------------------------- */
/* AUTHOR NAME: MACIEJ TOMASZEWSKI                    */
/* CREATE DATE: 02.12.2023                            */
/* DESCRIPTION:                                       */
/*  MY FIRST PYTHON & SQL PROJECT. THIS SCRIPT IS     */
/*  USED TO LOAD SAMPLE, FICTIOUS DATA TO TABLES      */
/*  CREATED FOR THE PURPOSE OF THE PROJECT.           */
/*  THIS IS THE THIRD OF 3 SQL SCRIPTS.               */

/*                  SQL SCRIPT 3/3                    */
/* -------------------------------------------------- */

-- previewing data
-- tables to insert data from flat files
SELECT * FROM MT_PythonSQL_Project1.dbo.Customers;
SELECT * FROM MT_PythonSQL_Project1.dbo.Sales;

-- tables used to prepare queries to insert tables for data & analyzes
SELECT * FROM MT_PythonSQL_Project1.dbo.CustVIP;
SELECT * FROM MT_PythonSQL_Project1.dbo.ItemsP;

-- tables for data & analyzes to load transformed data
SELECT * FROM MT_PythonSQL_Project1.dbo.CustAnalytics;
SELECT * FROM MT_PythonSQL_Project1.dbo.ItemsAnalytics;

-- queries (created in Python separately) used to insert tables for data & analyzes
-- CustAnalytics: detailed customers data
WITH _cte_CustVIP AS
	(
		SELECT CustomerID
		FROM MT_PythonSQL_Project1.dbo.CustVIP
		WHERE IsValid = 1
	)
SELECT 
	C.CustomerID AS CustID
	,C.CustomerName AS CustNm
	,C.Region AS Reg
	,CASE  
		WHEN VIP.CustomerID IS NOT NULL THEN 1 ELSE 0
	END AS IsVIP
FROM MT_PythonSQL_Project1.dbo.Customers AS C
LEFT JOIN _cte_CustVIP AS VIP 
	ON VIP.CustomerID = C.CustomerID
WHERE C.IsValid = 1
;

-- ItemsAnalytics: detailed sales data
WITH _cte_ItemsP AS
	(
		SELECT ItemID
		FROM MT_PythonSQL_Project1.dbo.ItemsP
		WHERE IsValid = 1
	)
SELECT
    S.ItemID
    ,S.ItemName
    ,S.Category
	,CASE
		WHEN PRM.ItemID IS NOT NULL THEN 1 ELSE 0
	END AS IsPremium
	,MAX(S.OrderDate) AS LastOrderDate
    ,SUM(S.Sales) AS [Sales2015_17]
    ,SUM(S.Profit) AS [Profit2015_17]
FROM MT_PythonSQL_Project1.dbo.Sales AS S
LEFT JOIN _cte_ItemsP AS PRM
	ON PRM.ItemID = S.ItemID
WHERE YEAR(S.OrderDate) BETWEEN 2015 AND 2017
GROUP BY
    S.ItemID
    ,S.ItemName
    ,S.Category
	,CASE
		WHEN PRM.ItemID IS NOT NULL THEN 1 ELSE 0
	END
;
