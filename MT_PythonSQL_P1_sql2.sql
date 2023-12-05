/* -------------------------------------------------- */
/* AUTHOR NAME: MACIEJ TOMASZEWSKI                    */
/* CREATE DATE: 02.12.2023                            */
/* DESCRIPTION:                                       */
/*  MY FIRST PYTHON & SQL PROJECT. THIS SCRIPT IS     */
/*  USED TO CREATE TABLES WITH SAMPLE DATA FOR	      */
/*  THE PURPOSE OF THE PROJECT. THIS IS THE           */
/*  SECOND OF 3 SQL SCRIPTS.                          */

/*                  SQL SCRIPT 2/3                    */
/* -------------------------------------------------- */

-- deleting tables if exist
-- stage tables
IF OBJECT_ID('MT_PythonSQL_Project1.dbo.Customers_stage') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.Customers_stage
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.Sales_stage') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.Sales_stage
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.CustAnalytics_stage') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.CustAnalytics_stage
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.ItemsAnalytics_stage') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.ItemsAnalytics_stage
END;

-- target tables
IF OBJECT_ID('MT_PythonSQL_Project1.dbo.Customers') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.Customers
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.CustVIP') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.CustVIP
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.Sales') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.Sales
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.ItemsP') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.ItemsP
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.CustAnalytics') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.CustAnalytics
END;

IF OBJECT_ID('MT_PythonSQL_Project1.dbo.ItemsAnalytics') IS NOT NULL
BEGIN
	DROP TABLE MT_PythonSQL_Project1.dbo.ItemsAnalytics
END;

-- creating tables
CREATE TABLE MT_PythonSQL_Project1.dbo.Customers
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,CustomerID INT
	,CustomerName VARCHAR(50)
	,City VARCHAR(50)
	,PostalCode INT
	,Region VARCHAR(10)
	,IsValid BIT
);

CREATE TABLE MT_PythonSQL_Project1.dbo.CustVIP
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,CustomerID INT
	,IsValid BIT
);

CREATE TABLE MT_PythonSQL_Project1.dbo.Sales
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,OrderDate DATE
	,ShipDate DATE
	,CustomerID INT
	,City VARCHAR(50)
	,PostalCode INT
	,Region VARCHAR(20)
	,ItemID INT
	,Category VARCHAR(50)
	,ItemName VARCHAR(100)
	,Sales DECIMAL(10,4)
	,Quantity INT
	,Profit DECIMAL(10,4)
)

CREATE TABLE MT_PythonSQL_Project1.dbo.ItemsP
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,ItemID INT
	,IsValid BIT
);

CREATE TABLE MT_PythonSQL_Project1.dbo.CustAnalytics
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,CustomerID INT
	,CustomerName VARCHAR(50)
	,Region VARCHAR(10)
	,IsVIP BIT
);

CREATE TABLE MT_PythonSQL_Project1.dbo.ItemsAnalytics
(
	DB_RowID INT IDENTITY(1,1)
	,DB_CreateTimestamp DATETIME
	,DB_CreateUser VARCHAR(100)
	,ItemID INT
	,ItemName VARCHAR(100)
	,Category VARCHAR(50)
	,IsPremium BIT	
	,TotalSales DECIMAL(10,4)
	,TotalProfit DECIMAL(10,4)
);

-- inserting data
INSERT INTO MT_PythonSQL_Project1.dbo.Customers(DB_CreateTimestamp, DB_CreateUser, CustomerID, CustomerName, City, PostalCode, Region, IsValid)
VALUES
	(GETDATE(), 'name.surname@company.com', '10105', 'John Kowalsky', 'Pearland', 19143, 'Central', 1)
	,(GETDATE(), 'name.surname@company.com', '17890', 'Peter Nowak', 'Philadelphia', 97224, 'Central', 1)
	,(GETDATE(), 'name.surname@company.com', '14290', 'Janush Nosache', 'Lawrence', 19143, 'South', 1)
	,(GETDATE(), 'name.surname@company.com', '11605', 'Matt Brown', 'Springfield', 1841, 'South', 0)
	,(GETDATE(), 'name.surname@company.com', '10990', 'Kate Clark', 'Columbus', 85635, 'East', 0)
;

INSERT INTO MT_PythonSQL_Project1.dbo.CustVIP(DB_CreateTimestamp, DB_CreateUser, CustomerID, IsValid)
VALUES 
	(GETDATE(), 'name.surname@company.com', '10105', 1)
	,(GETDATE(), 'name.surname@company.com', '11605', 1)
;

INSERT INTO MT_PythonSQL_Project1.dbo.Sales(DB_CreateTimestamp, DB_CreateUser, OrderDate, ShipDate, CustomerID, City, PostalCode, Region, ItemID, Category, ItemName, Sales, Quantity, Profit)
VALUES
	(GETDATE(), 'name.surname@company.com', '2017-06-05', '2017-06-06', 10105, 'Pearland', 77581, 'Central', 10001619, 'Technology', 'LG G3', 470.376, 3, 52.9173)
	,(GETDATE(), 'name.surname@company.com', '2017-05-30', '2017-06-03', 17890, 'Tigard', 97224, 'West', 10001619, 'Technology', 'LG G3', 156.792, 1, 17.6391)
	,(GETDATE(), 'name.surname@company.com', '2016-12-23', '2016-12-30', 14290, 'Philadelphia', 19143, 'East', 10002645, 'Technology', 'LG G2', 1499.97, 5, -374.9925)
	,(GETDATE(), 'name.surname@company.com', '2015-12-26', '2016-01-02', 11605, 'Lawrence', 1841, 'East', 10002815, 'Office Supplies', 'Staples', 22.2, 5, 10.434)
	,(GETDATE(), 'name.surname@company.com', '2016-04-01', '2016-04-08', 10990, 'Sierra Vista', 85635, 'West', 10003112, 'Office Supplies', 'Staples', 31.56, 5, 8.76)
	,(GETDATE(), 'name.surname@company.com', '2016-01-11', '2016-01-13', 13255, 'Springfield', 45503, 'East', 10001166, 'Office Supplies', 'Xerox 2', 15.552, 3, 5.4432)
	,(GETDATE(), 'name.surname@company.com', '2015-12-14', '2015-12-16', 13630, 'Portland', 97206, 'West', 10001051, 'Technology', 'HTC One', 123.45, 4, -35.9964)
	,(GETDATE(), 'name.surname@company.com', '2015-04-20', '2015-04-25', 21160, 'Columbus', 31907, 'South', 10001166, 'Office Supplies', 'Xerox 2', 12.96, 2, 6.2208)
	,(GETDATE(), 'name.surname@company.com', '2015-08-23', '2015-08-23', 20995, 'Bolingbrook', 60440, 'Central', 10003021, 'Office Supplies', 'Staples', 12.032, 8, 2.256)
	,(GETDATE(), 'name.surname@company.com', '2014-10-02', '2014-10-05', 14080, 'Tempe', 85281, 'West', 10000735, 'Office Supplies', 'Staples', 4.672, 2, 1.46)
;

INSERT INTO MT_PythonSQL_Project1.dbo.ItemsP(DB_CreateTimestamp, DB_CreateUser, ItemID, IsValid)
VALUES 
	(GETDATE(), 'name.surname@company.com', 10001619, 1)
	,(GETDATE(), 'name.surname@company.com', 10001166, 1)
;

-- previewing data
SELECT * FROM MT_PythonSQL_Project1.dbo.Customers;
SELECT * FROM MT_PythonSQL_Project1.dbo.CustVIP;
SELECT * FROM MT_PythonSQL_Project1.dbo.Sales;
SELECT * FROM MT_PythonSQL_Project1.dbo.ItemsP;
SELECT * FROM MT_PythonSQL_Project1.dbo.CustAnalytics;
SELECT * FROM MT_PythonSQL_Project1.dbo.ItemsAnalytics;
