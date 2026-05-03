-- ------------------------------------------------------------------
-- 0). First, How Many Rows are in the Products Table?
-- ------------------------------------------------------------------
SELECT COUNT(*) AS Num_Products FROM northwind.products; 

-- ------------------------------------------------------------------
-- 1). Product Name and Unit/Quantity
-- ------------------------------------------------------------------
SELECT product_name, 
quantity_per_unit 
FROM northwind.products;

-- ------------------------------------------------------------------
-- 2). Product ID and Name of Current Products
-- ------------------------------------------------------------------
SELECT id AS product_id, 
product_name 
FROM northwind.products 
WHERE discontinued <> 1;


-- ------------------------------------------------------------------
-- 3). Product ID and Name of Discontinued Products
-- ------------------------------------------------------------------
SELECT id AS product_id,
product_name 
FROM northwind.products 
WHERE discontinued != 0;

-- ------------------------------------------------------------------
-- 4). Name & List Price of Most & Least Expensive Products
-- ------------------------------------------------------------------
SELECT product_name, list_price 
FROM northwind.products
WHERE list_price = (SELECT MIN(list_price) FROM northwind.products) 
or list_price = (SELECT MAX(list_price) FROM northwind.products);

-- ------------------------------------------------------------------
-- 5). Product ID, Name & List Price Costing Less Than $20
-- ------------------------------------------------------------------
SELECT id AS product_id, product_name, list_price 
FROM northwind.products
WHERE list_price < 20.00
ORDER BY list_price DESC;

-- ------------------------------------------------------------------
-- 6). Product ID, Name & List Price Costing Between $15 and $20
-- ------------------------------------------------------------------
SELECT id AS product_id, product_name, list_price
FROM northwind.products
WHERE list_price BETWEEN 15 AND 20
ORDER BY list_price DESC;


-- ------------------------------------------------------------------
-- 7). Product Name & List Price Costing Above Average List Price
-- ------------------------------------------------------------------
SELECT product_name, list_price
FROM northwind.products
WHERE list_price > (SELECT AVG(list_price) FROM northwind.products)
ORDER BY list_price DESC;

-- ------------------------------------------------------------------
-- 8). Product Name & List Price of 10 Most Expensive Products 
-- ------------------------------------------------------------------
SELECT product_name, list_price
FROM northwind.products
ORDER BY list_price DESC
LIMIT 10;

-- ------------------------------------------------------------------ 
-- 9). Count of Current and Discontinued Products 
-- ------------------------------------------------------------------
UPDATE northwind.products SET discontinued = 1 where id = 95;
SELECT CASE discontinued
	when 1 THEN 'yes'
    else 'no'
    end as is_discontinued,
    COUNT(*) as product_count
FROM northwind.products
GROUP BY discontinued;

UPDATE northwind.products SET discontinued = 0 WHERE id= 95;


-- ------------------------------------------------------------------
-- 10). Product Name, Units on Order and Units in Stock
--      Where Quantity In-Stock is Less Than the Quantity On-Order. 
-- ------------------------------------------------------------------
SELECT product_name, 
reorder_level AS units_in_stock, 
target_level AS units_on_order
from northwind.products
WHERE reorder_level < target_level ;


-- ------------------------------------------------------------------
-- EXTRA CREDIT -----------------------------------------------------
-- ------------------------------------------------------------------


-- ------------------------------------------------------------------
-- 11). Products with Supplier Company & Address Info
-- ------------------------------------------------------------------
SELECT p.product_name,
p.list_price AS product_list_price,
p.category AS product_category,
s.company AS supplier_company
,s.address AS supplier_address
,s.city AS supplier_city
,s.state_province AS supplier_state_province
,s.zip_postal_code AS supplier_zip_postal_code
FROM northwind.suppliers AS s
INNER JOIN northwind.products AS p
ON s.id = p.supplier_ids;


-- ------------------------------------------------------------------
-- 12). Number of Products per Category With Less Than 5 Units
-- ------------------------------------------------------------------
SELECT category
, COUNT(*) as units_in_stock
FROM northwind.products
GROUP BY category
HAVING units_in_stock < 5;


-- ------------------------------------------------------------------
-- 13). Number of Products per Category Priced Less Than $20.00
-- ------------------------------------------------------------------
SELECT category
, COUNT(*) as product_count
FROM northwind.products
WHERE list_price < 20
GROUP BY category
ORDER BY category;