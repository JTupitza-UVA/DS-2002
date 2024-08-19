-- ----------------------------------------------------------------------------
-- 1). First, How Many Rows (Products) are in the Products Table?
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 2). Fetch Each Product Name and its Quantity per Unit
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 3). Fetch the Product ID and Name of Currently Available Products
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 4). Fetch the Product ID, Name & List Price Costing Less Than $20
--     Sort the results with the most expensive Products first.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 5). Fetch the Product ID, Name & List Price Costing Between $15 and $20
--     Sort the results with the most expensive Products first.
-- ----------------------------------------------------------------------------


-- Older (Equivalent) Syntax -----


-- ----------------------------------------------------------------------------
-- 6). Fetch the Product Name & List Price of the 10 Most Expensive Products 
--     Sort the results with the most expensive Products first.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 7). Fetch the Name & List Price of the Most & Least Expensive Products
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 8). Fetch the Product Name & List Price Costing Above Average List Price
--     Sort the results with the most expensive Products first.
-- ----------------------------------------------------------------------------


-- ---------------------------------------------------------------------------- 
-- 9). Fetch & Label the Count of Current and Discontinued Products using
-- 	   the "CASE... WHEN" syntax to create a column named "availablity"
--     that contains the values "discontinued" and "current". 
-- ----------------------------------------------------------------------------
UPDATE northwind.products SET discontinued = 1 WHERE id IN (95, 96, 97);

-- To Do: Author Query

UPDATE northwind.products SET discontinued = 0 WHERE id in (95, 96, 97);

-- ----------------------------------------------------------------------------
-- 10). Fetch Product Name, Reorder Level, Target Level and "Reorder Threshold"
-- 	    Where Reorder Level is Less Than or Equal to 20% of Target Level
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 11). Fetch the Number of Products per Category Priced Less Than $20.00
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 12). Fetch the Number of Products per Category With Less Than 5 Units In Stock
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 13). Fetch the Products along with their Supplier Company & Address Info
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 14). Fetch the Customer ID and Full Name for All Customers along with
-- 		the Order ID and Order Date for Any Orders they may have
-- ----------------------------------------------------------------------------



-- ----------------------------------------------------------------------------
-- 15). Fetch the Order ID and Order Date for All Orders along with
--   	the Customr ID and Full Name for Any Associated Customers
-- ----------------------------------------------------------------------------


