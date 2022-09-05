-- -------------------------------------------------------------------------
-- DERIVED TABLE EXPRESSION: Scope of Alias is this Outer Query ONLY 
-- -------------------------------------------------------------------------
SELECT o.* FROM (
	SELECT id AS order_id, customer_id, employee_id, order_date
	FROM northwind.orders
) AS o
ORDER BY order_date DESC;
    
-- -------------------------------------------------------------------------
-- COMMON TABLE EXPRESSION: Scope of Alias is all subsequent queries in batch
-- -------------------------------------------------------------------------
WITH OrdersCTE AS (
	SELECT id AS order_id, customer_id, employee_id, order_date
	FROM northwind.orders
)
SELECT * FROM OrdersCTE ORDER BY order_date DESC;

-- Here the alias' are declared in the CTE definition
WITH OrdersCTE (order_id, customer_id, employee_id, order_date) AS (
	SELECT id, customer_id, employee_id, order_date
	FROM northwind.orders
)
SELECT * FROM OrdersCTE ORDER BY order_date DESC;


-- -------------------------------------------------------------------------
-- JOINING Tables: 
-- -------------------------------------------------------------------------
SELECT COUNT(*) AS CustomerCount FROM northwind.customers;  -- 29
SELECT COUNT(*) AS OrderCount FROM northwind.orders;        -- 48

-- Fetch All Customers and Any Orders they may have -------------
SELECT COUNT(*) FROM (
	SELECT c.id AS customer_id, c.last_name
		, o.id AS order_id, o.order_date
	FROM northwind.customers AS c
	LEFT OUTER JOIN northwind.orders AS o
	ON c.id = o.customer_id
	ORDER BY customer_id
) AS co;

-- Fetch Customers with Corresponding Orders --------------------
WITH CustomerOrdersCTE AS (
	SELECT c.id AS customer_id, c.last_name
		, o.id AS order_id, o.order_date
	FROM northwind.customers AS c
	INNER JOIN northwind.orders AS o
	ON c.id = o.customer_id
	ORDER BY customer_id
)
SELECT COUNT(*) FROM (CustomerOrdersCTE);

-- Fetch Orders and Any Customers -------------------------------
SELECT COUNT(*) FROM (
	SELECT c.id AS customer_id, c.last_name
		, o.id AS order_id, o.order_date
	FROM northwind.customers AS c
	RIGHT OUTER JOIN northwind.orders AS o
	ON c.id = o.customer_id
	ORDER BY customer_id
) AS co;


-- -------------------------------------------------------------------------
-- SET-BASED OPERATIONS: UNION (Distinct Rows)
-- -------------------------------------------------------------------------
SELECT COUNT(*) FROM (

	SELECT city, state_province, country_region
	FROM northwind.employees
	UNION
	SELECT city, state_province, country_region
	FROM northwind.customers
    
) AS distinct_addresses;

-- -------------------------------------------------------------------------
-- SET-BASED OPERATIONS: UNION ALL (Keeps Duplicates)
-- -------------------------------------------------------------------------
SELECT COUNT(*) FROM (

	SELECT city, state_province, country_region
	FROM northwind.employees
	UNION ALL
	SELECT city, state_province, country_region
	FROM northwind.customers
    
) AS all_addresses;


-- -------------------------------------------------------------------------
-- Self-contained Subqueries:
-- - Inner Query is Evaluated only once
-- - Inner Query Can be executed independent of the outer query
-- -------------------------------------------------------------------------
SELECT id, last_name FROM northwind.customers;

SELECT id AS order_id, customer_id, order_date
FROM northwind.orders
WHERE customer_id =
	(SELECT id FROM northwind.customers
    WHERE last_name LIKE 'Edwards');   -- Returns Scalar
#    WHERE last_name LIKE 'D%');        -- Returns NULL
#    WHERE last_name LIKE 'Lee');       -- Returns Multiple

-- -------------------------------------------------------------------------
-- Corelated Subqueries: Inner & Outer Queries are dependent on each other
-- - Inner Query references column(s) in the Outer Query
-- - Inner Query evaluated once for each row returned by the Outer Query
-- -------------------------------------------------------------------------

-- Return the most recent order(s) for each Customer -----------------------
		SELECT id AS order_id, customer_id, order_date
		FROM northwind.orders AS o1
		WHERE order_date =
			(SELECT MAX(order_date)
			FROM northwind.orders AS o2
			WHERE o2.customer_id = o1.customer_id)
		ORDER BY customer_id;

-- TIEBREAKER: Use a second correlated subquery to
-- return only the most recent order for each customer
SELECT id AS order_id, customer_id, order_date
FROM northwind.orders AS o1
WHERE order_date =
	(SELECT MAX(order_date)
    FROM northwind.orders AS o2
    WHERE o2.customer_id = o1.customer_id)
AND id =
	(SELECT MAX(id)
    FROM northwind.orders AS o2
    WHERE o2.customer_id = o1.customer_id
		AND o2.order_date = o1.order_date)
ORDER BY customer_id;

-- TIEBREAKER: Use a second NESTED Correlated Subquery
-- to return only the most recent order for each customer
SELECT id AS order_id, customer_id, order_date
FROM northwind.orders AS o1
WHERE id =
	(SELECT MAX(id)
    FROM northwind.orders AS o2
    WHERE o2.customer_id = o1.customer_id
		AND o2.order_date =
			(SELECT MAX(order_date)
			FROM northwind.orders AS o3
			WHERE o3.customer_id = o1.customer_id))
ORDER BY customer_id;

-- -------------------------------------------------------------------------
-- EXISTS Enables efficient verification of records EXISTING in the subquery
-- - Subquery is frequently, but not necessarily, correlated.
-- -------------------------------------------------------------------------
SELECT id AS customer_id, company
FROM northwind.customers AS c
WHERE EXISTS
	(SELECT * FROM northwind.orders AS o
    WHERE o.customer_id = c.id);

