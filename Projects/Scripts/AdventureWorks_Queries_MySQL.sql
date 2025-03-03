USE adventureworks;

-- ----------------------------------------------------------------------------------
-- Get Customer Data ----------------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_customers_vw AS
(
	SELECT c.CustomerID
		, c.AccountNumber
		, c.CustomerType
		, t.Name AS AddressType
		, a.AddressLine1
		, a.AddressLine2
		, a.City
		, sp.StateProvinceCode
		, sp.Name AS `State_Province`
		, sp.IsOnlyStateProvinceFlag
		, a.PostalCode
		, sp.CountryRegionCode
		, cr.Name AS `Country_Region`
		, st.Group AS `Sales Territory Group`
		, st.Name AS `Sales Territory`
	FROM customer AS c
	INNER JOIN customeraddress AS ca
	ON c.CustomerID = ca.CustomerID
	INNER JOIN address AS a
	ON ca.AddressID = a.AddressID
	INNER JOIN addresstype AS t
	ON ca.AddressTypeID = t.AddressTypeID
	INNER JOIN stateprovince AS sp
	ON sp.StateProvinceID = a.StateProvinceID
	INNER JOIN countryregion AS cr
	ON sp.CountryRegionCode = cr.CountryRegionCode
	INNER JOIN salesterritory AS st
	ON c.TerritoryID = st.TerritoryID
);

SELECT * FROM dim_customers_vw;

-- ----------------------------------------------------------------------------------
-- Get Employee Data ----------------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_employee_vw AS
(
	SELECT e.EmployeeID,
		e.NationalIDNumber,
		e.LoginID,
		e.ManagerID,
		c.FirstName,
		c.MiddleName,
		c.LastName,
		e.Title,
		c.EmailAddress,
		c.EmailPromotion,
		c.Phone,
		e.BirthDate,
		e.MaritalStatus,
		e.Gender,
		e.HireDate,
		e.SalariedFlag,
		e.VacationHours,
		e.SickLeaveHours,
		e.CurrentFlag
	FROM employee AS e
	INNER JOIN contact AS c
	ON e.ContactID = c.ContactID
);

SELECT * FROM dim_employee_vw;

-- ----------------------------------------------------------------------------------
-- Get Product Data -----------------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_products_vw AS
(
	WITH ProductCatsAndSubCatsCTE AS 
	(
		SELECT pc.ProductCategoryID
			, pc.Name AS ProductCategory
			, psc.ProductSubcategoryID
			, psc.Name AS ProductSubcategory
		FROM productcategory AS pc
		INNER JOIN productsubcategory AS psc
		ON pc.ProductCategoryID = psc.ProductCategoryID
	)
	SELECT p.ProductID,
		p.Name,
		p.ProductNumber,
		p.MakeFlag,
		p.FinishedGoodsFlag,
		p.Color,
		p.SafetyStockLevel,
		p.ReorderPoint,
		p.StandardCost,
		p.ListPrice,
		p.Size,
		p.SizeUnitMeasureCode,
		p.WeightUnitMeasureCode,
		p.Weight,
		p.DaysToManufacture,
		p.ProductLine,
		p.Class,
		p.Style,
		psc.ProductCategory,
		psc.ProductSubcategory,
		pm.NAME AS ProductModel,
		p.SellStartDate,
		p.SellEndDate,
		p.DiscontinuedDate
	FROM product AS p
	LEFT OUTER JOIN ProductCatsAndSubCatsCTE AS psc
	ON p.ProductSubcategoryID = psc.ProductSubcategoryID
	LEFT OUTER JOIN productmodel AS pm
	ON p.ProductModelID = pm.ProductModelID
);

SELECT * FROM dim_products_vw;

-- ----------------------------------------------------------------------------------
-- Get Vendor Data -----------------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_vendors_vw AS
(
	SELECT v.VendorID
		, v.AccountNumber
		, v.Name
		, v.CreditRating
		, v.PreferredVendorStatus
		, v.ActiveFlag
		, t.Name AS AddressType
		, a.AddressLine1
		, a.AddressLine2
		, a.City
		, sp.StateProvinceCode
		, sp.Name AS `State_Province`
		, a.PostalCode
	FROM vendor AS v
	INNER JOIN vendoraddress AS va
	ON v.VendorID = va.VendorID
	INNER JOIN address AS a
	ON va.AddressID = a.AddressID
	INNER JOIN stateprovince AS sp
	ON sp.StateProvinceID = a.StateProvinceID
	INNER JOIN addresstype AS t
	ON va.AddressTypeID = t.AddressTypeID
);

SELECT * FROM dim_vendors_vw;

-- ----------------------------------------------------------------------------------
-- Get Purchase Order Data ----------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW fact_purchase_orders_vw AS
(
	SELECT poh.PurchaseOrderID
		, poh.RevisionNumber
		, poh.Status
		, poh.EmployeeID
		, poh.VendorID
		, pod.ProductID
		, pod.OrderQty
		, pod.UnitPrice
		, pod.LineTotal
		, poh.OrderDate
		, sm.Name AS ShipMethod
		, sm.ShipBase
		, sm.ShipRate
		, poh.ShipDate
		, poh.SubTotal
		, poh.TaxAmt
		, poh.Freight
		, poh.TotalDue
		, pod.DueDate
		, pod.ReceivedQty
		, pod.RejectedQty
		, pod.StockedQty
	FROM purchaseorderheader AS poh
	INNER JOIN shipmethod AS sm
	ON poh.ShipMethodID = sm.ShipMethodID
	LEFT OUTER JOIN purchaseorderdetail AS pod
	ON poh.PurchaseOrderID = pod.PurchaseOrderID
);

-- ----------------------------------------------------------------------------------
-- Get Sales Order Data -------------------------------------------------------------
-- ----------------------------------------------------------------------------------
CREATE OR REPLACE VIEW fact_sales_orders_vw AS
(
	SELECT soh.SalesOrderID,
		soh.RevisionNumber,
		soh.OrderDate,
		soh.DueDate,
		soh.ShipDate,
		soh.Status,
		soh.OnlineOrderFlag,
		soh.SalesOrderNumber,
		soh.PurchaseOrderNumber,
		soh.AccountNumber,
		soh.CustomerID,
		soh.ContactID,
		soh.SalesPersonID,
		st.Group AS `Sales Territory Group`,
		st.Name AS `Sales Territory`,
		soh.BillToAddressID,
		soh.ShipToAddressID,
		sm.Name AS ShipMethod,
		sm.ShipBase,
		sm.ShipRate,
		cc.CardType AS `Credit Card Type`,
		cc.CardNumber AS `Credit Card Number`,
		cc.ExpMonth AS `Credit Card ExpMonth`,
		cc.ExpYear AS `Credit Card ExpYear`,
		soh.CreditCardApprovalCode,
		soh.SubTotal,
		soh.TaxAmt,
		soh.Freight,
		soh.TotalDue,
		sod.CarrierTrackingNumber,
		sod.OrderQty,
		sod.ProductID,
		sod.UnitPrice,
		sod.LineTotal
	FROM salesorderheader AS soh
	LEFT OUTER JOIN salesorderdetail AS sod
	ON soh.SalesOrderID = sod.SalesOrderID
	LEFT OUTER JOIN creditcard AS cc
	ON soh.CreditCardID = cc.CreditCardID
	LEFT OUTER JOIN shipmethod AS sm
	ON soh.ShipMethodID = sm.ShipMethodID
	INNER JOIN salesterritory AS st
	ON soh.TerritoryID = st.TerritoryID
);

SELECT COUNT(*) FROM salesorderheader; #31465
SELECT COUNT(*) FROM salesorderdetail; #121317