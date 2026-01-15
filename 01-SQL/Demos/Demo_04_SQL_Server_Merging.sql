USE tempdb;

-- --------------------------------------------------------------------------------------
-- SETUP the Customers and CustomersStage tables to facilitate the demo
-- --------------------------------------------------------------------------------------

-- --------------------------------------------------------------------------------------
-- 1. Target Table ----------------------------------------------------------------------
-- --------------------------------------------------------------------------------------
IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;

CREATE TABLE dbo.Customers
(
    custid      INT         NOT NULL,
    companyname VARCHAR(25) NOT NULL,
    phone       VARCHAR(20) NOT NULL,
    address     VARCHAR(50) NOT NULL,
    CONSTRAINT PK_Customers PRIMARY KEY(custid)
);

INSERT INTO dbo.Customers(custid, companyname, phone, address)
VALUES
(1, 'cust 1', '(111) 111-1111', 'address 1'),
(2, 'cust 2', '(222) 222-2222', 'address 2'),
(3, 'cust 3', '(333) 333-3333', 'address 3'),
(4, 'cust 4', '(444) 444-4444', 'address 4'),
(5, 'cust 5', '(555) 555-5555', 'address 5');


-- --------------------------------------------------------------------------------------
-- 2. Staging Table (contains Updated and New rows Only - Like a CDC table --------------
-- --------------------------------------------------------------------------------------
IF OBJECT_ID('dbo.CustomersStage', 'U') IS NOT NULL DROP TABLE dbo.CustomersStage;

CREATE TABLE dbo.CustomersStage
(
    custid      INT         NOT NULL,
    companyname VARCHAR(25) NOT NULL,
    phone       VARCHAR(20) NOT NULL,
    address     VARCHAR(50) NOT NULL,
    CONSTRAINT PK_CustomersStage PRIMARY KEY(custid)
);

INSERT INTO dbo.CustomersStage(custid, companyname, phone, address)
VALUES
(2, 'AAAAA', '(222) 222-2222', 'address 2'),
(3, 'BBBBB', '(333) 333-3333', 'address 3'),
(5, 'CCCCC', 'DDDDD', 'EEEEE'),
(6, 'cust 6 (new)', '(666) 666-6666', 'address 6'),
(7, 'cust 7 (new)', '(777) 777-7777', 'address 7');


-- ------------------------------------------------------------
-- Unit Test the Newly Created Tables
-- ------------------------------------------------------------
SELECT * FROM dbo.Customers;
SELECT * FROM dbo.CustomersStage;


-- --------------------------------------------------------------------------------------
-- Demonstrate the MERGE Statements' Behavior
-- --------------------------------------------------------------------------------------
MERGE INTO dbo.Customers AS TGT
USING dbo.CustomersStage AS SRC
    ON TGT.custid = SRC.custid
WHEN MATCHED THEN
    UPDATE SET
        TGT.companyname = SRC.companyname,
        TGT.phone = SRC.phone,
        TGT.address = SRC.address
WHEN NOT MATCHED THEN
    INSERT (custid, companyname, phone, address)
    VALUES (SRC.custid, SRC.companyname, SRC.phone, SRC.address);


-- ------------------------------------------------------------
-- Unit Test the Customers table
-- ------------------------------------------------------------
SELECT * FROM dbo.Customers;


-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP TABLE dbo.Customers;
DROP TABLE dbo.CustomersStage;




-- --------------------------------------------------------------------------------------
-- This demonstrates MERGE in a Stored Procedure with OUTPUT values in a Temp Table
-- --------------------------------------------------------------------------------------

USE AdventureWorks2022
GO


-- Create a temporary table to hold the updated or inserted values
-- from the OUTPUT clause.
CREATE TABLE #MyTempTable (
    ExistingCode NCHAR(3),
    ExistingName NVARCHAR(50),
    ExistingDate DATETIME,
    ActionTaken NVARCHAR(10),
    NewCode NCHAR(3),
    NewName NVARCHAR(50),
    NewDate DATETIME
);
GO

CREATE PROCEDURE dbo.InsertUnitMeasure @UnitMeasureCode NCHAR(3),
    @Name NVARCHAR(25)
AS
BEGIN
    SET NOCOUNT ON;

    MERGE Production.UnitMeasure AS tgt
    USING (SELECT @UnitMeasureCode, @Name) AS src(UnitMeasureCode, Name)
        ON (tgt.UnitMeasureCode = src.UnitMeasureCode)
    WHEN MATCHED
        THEN
            UPDATE
            SET Name = src.Name
    WHEN NOT MATCHED
        THEN
            INSERT (UnitMeasureCode, Name)
            VALUES (src.UnitMeasureCode, src.Name)
    OUTPUT deleted.*,
        $action,
        inserted.*
    INTO #MyTempTable;
END;
GO

-- ------------------------------------------------------------
-- Unit Test the procedure and return the results.
-- ------------------------------------------------------------
EXEC InsertUnitMeasure @UnitMeasureCode = 'ABC', @Name = 'New Test Value';
EXEC InsertUnitMeasure @UnitMeasureCode = 'XYZ', @Name = 'Test Value';
EXEC InsertUnitMeasure @UnitMeasureCode = 'ABC', @Name = 'Another Test Value';


-- ------------------------------------------------------------
-- Verify Correct Output Values
-- ------------------------------------------------------------
SELECT * FROM #MyTempTable;


-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DELETE FROM Production.UnitMeasure
WHERE UnitMeasureCode IN ('ABC', 'XYZ');

DROP TABLE #MyTempTable;
GO