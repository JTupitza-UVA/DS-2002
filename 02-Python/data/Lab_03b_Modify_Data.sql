USE northwind;

-- -----------------------------------------------------------------
-- 1. Update the Customers Dimension
-- -----------------------------------------------------------------
-- --------------------------------------------------
-- First Update Two Existing Customers
-- --------------------------------------------------
UPDATE northwind.customers
SET company = 'Company BB'
	, last_name = 'Raghav'
	, first_name = 'Amritansh'
	, job_title = 'Purchasing Manager'
	, business_phone = '(901)555-0100' 	-- Update Area Codes
	, fax_number = '(901)555-0101'
	, address = '789 28th Street'
	, city = 'Memphis'
	, state_province = 'TN'
	, zip_postal_code = '37544' 		-- Update Zip Codes
	, country_region = 'USA'
WHERE id = 28;

UPDATE northwind.customers
SET company = 'Company CC'
	, last_name = 'Lee'
	, first_name = 'Soo Jung'
	, job_title = 'Purchasing Manager'
	, business_phone = '(303)555-0100'
	, fax_number = '(303)555-0101'
	, address = '789 29th Street'
	, city = 'Denver'
	, state_province = 'CO'
	, zip_postal_code = '80033'
	, country_region = 'USA'
WHERE id = 29;

-- --------------------------------------------------
-- Next Insert Two New Customers
-- --------------------------------------------------
INSERT INTO northwind.customers (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (30, 'Company DD', 'Jagger', 'Mick', NULL, 'Purchasing Manager', '(901)555-0100', NULL, NULL, '(901)555-0101', '789 30th Street', 'Memphis', 'TN', '37544', 'USA', NULL, NULL, '');

INSERT INTO northwind.customers (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (31, 'Company EE', 'Page', 'Jimmy', NULL, 'Purchasing Manager', '(303)555-0100', NULL, NULL, '(303)555-0101', '789 31th Street', 'Denver', 'CO', '80033', 'USA', NULL, NULL, '');

-----------------------------------------------------
-- Verify the Changes Were Made
-- --------------------------------------------------
SELECT * FROM northwind.customers
WHERE id >= 28;

SELECT * FROM northwind_dw2.dim_customers
WHERE customer_id >= 28;


-- -----------------------------------------------------------------
-- 2. Update the Employees Dimension
-- -----------------------------------------------------------------
-- --------------------------------------------------
-- First Update Two Existing Employees
-- --------------------------------------------------
UPDATE northwind.employees
SET company = 'Northwind Traders',
	last_name = 'Giussani',
    first_name = 'Laura',
    email_address = 'laura@northwindtraders.com',
    job_title = 'Sales Coordinator',
    business_phone = '(425)555-0100',	-- Update Area Codes
    home_phone = '(425)555-0102',
    fax_number = '(425)555-0103',
    address = '123 8th Avenue',
    city = 'Redmond',
    state_province = 'WA',
    zip_postal_code = 98053,			-- Update Zip Codes
    country_region = 'USA'
WHERE id = 8;

UPDATE northwind.employees
SET company = 'Northwind Traders',
	last_name = 'Hellung-Larsen',
    first_name = 'Anne',
    email_address = 'anne@northwindtraders.com',
    job_title = 'Sales Representative',
    business_phone = '(564)555-0100',	-- Update Area Codes
    home_phone = '(564)555-0102',
    fax_number = '(564)555-0103',
    address = '123 9th Avenue',
    city = 'Seattle',
    state_province = 'WA',
    zip_postal_code = 98101,			-- Update Zip Codes
    country_region = 'USA'
WHERE id = 9;

-- --------------------------------------------------
-- Next Insert Two New Employees
-- --------------------------------------------------
INSERT INTO northwind.employees (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (10, 'Northwind Traders', 'Richards', 'Keith', 'keith@northwindtraders.com', 'Sales Coordinator', '(425)555-0100', '(425)555-0102', NULL, '(425)555-0103', '123 10th Avenue', 'Redmond', 'WA', '98053', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Reads and writes French.', '');

INSERT INTO northwind.employees (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (11, 'Northwind Traders', 'Jagger', 'Mick', 'mick@northwindtraders.com', 'Sales Representative', '(564)555-0100', '(564)555-0102', NULL, '(564)555-0103', '123 11th Avenue', 'Seattle', 'WA', '98101', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#', 'Fluent in French and German.', '');

-----------------------------------------------------
-- Verify the Changes Were Made
-- --------------------------------------------------
SELECT * FROM northwind.employees
WHERE id >= 8;

SELECT * FROM northwind_dw2.dim_employees
WHERE employee_id >= 8;


-- -----------------------------------------------------------------
-- 3. Update the Products Dimension
-- -----------------------------------------------------------------
-- --------------------------------------------------
-- First Update Two Existing Products
-- --------------------------------------------------
UPDATE northwind.products
SET supplier_ids = 6,
	product_code = 'NWTSO-98',
    product_name = 'Northwind Traders Vegetable Soup',
    description = 'Heart Healthy Savory Soup',		-- Add Description
    standard_cost = 1,
    list_price = 1.89,
    reorder_level = 100,
    target_level = 200,
    quantity_per_unit = 25,							-- Add Quantity per Unit
    discontinued = 0,
    minimum_reorder_quantity = 100,					-- Add Min Reorder Qty
    category = 'Soups'
WHERE id = 98;

UPDATE northwind.products
SET supplier_ids = 6,
	product_code = 'NWTSO-99',
    product_name = 'Northwind Traders Chicken Soup',
    description = 'Delicious Savory Soup',			-- Add Description
    standard_cost = 1,
    list_price = 1.95,
    reorder_level = 100,
    target_level = 200,
    quantity_per_unit = 25,							-- Add Quantity per Unit
    discontinued = 0,
    minimum_reorder_quantity = 100,					-- Add Min Reorder Qty
    category = 'Soups'
WHERE id = 99;

-- --------------------------------------------------
-- Next Insert Two New Products
-- --------------------------------------------------
INSERT INTO northwind.products (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`)
VALUES ('6', 100, 'NWTSO-100', 'Northwind Traders Ministrone Soup', 'Savory Traditional Vegetarian Soup', 1, 1.89, 100, 200, 25, 0, 100, 'Soups', '');

INSERT INTO northwind.products (`supplier_ids`, `id`, `product_code`, `product_name`, `description`, `standard_cost`, `list_price`, `reorder_level`, `target_level`, `quantity_per_unit`, `discontinued`, `minimum_reorder_quantity`, `category`, `attachments`)
VALUES ('6', 101, 'NWTSO-101', 'Northwind Traders Italian Wedding Soup', 'Savory Traditional Soup with Meatballs', 1, 1.95, 100, 200, 25, 0, 100, 'Soups', '');

-----------------------------------------------------
-- Verify the Changes Were Made
-- --------------------------------------------------
SELECT * FROM northwind.products
WHERE id >= 98;

SELECT * FROM northwind_dw2.dim_products
WHERE product_id >= 98;


-- -----------------------------------------------------------------
-- 4. Update the Shippers Dimension
-- -----------------------------------------------------------------
-- --------------------------------------------------
-- First Update Two Existing Shippers
-- --------------------------------------------------
UPDATE northwind.shippers
SET company = 'Shipping Company B',
	last_name = 'Plant',
    first_name = 'Robert',
    email_address = 'rplant@companyb.com',
    job_title = 'Shipping Agent',
    business_phone = '(901)434-0100',		-- Update Area Codes
    fax_number = '(901)434-0101',
    address = '123 2nd Street',
    city = 'Memphis',
    state_province = 'TN',
    zip_postal_code = '37544',				-- Update Zip Codes
    country_region = 'USA'
WHERE id = 2;

UPDATE northwind.shippers
SET company = 'Shipping Company C',
	last_name = 'Bonham',
    first_name = 'John',
    email_address = 'jbonham@companyc.com',
    job_title = 'Shipping Agent',
    business_phone = '(901)212-0100',		-- Update Area Codes
    fax_number = '(901)212-0101',
    address = '123 3rd Street',
    city = 'Memphis',
    state_province = 'TN',
    zip_postal_code = '37544',				-- Update Zip Codes
    country_region = 'USA'
WHERE id = 3;

-- --------------------------------------------------
-- Next Insert Two New Shippers
-- --------------------------------------------------
INSERT INTO northwind.shippers (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (4, 'Shipping Company D', 'Jones', 'John-Paul', 'jjones@companyd.com', 'Shipping Agent', '(901)343-0100', NULL, NULL, '(901)343-0101', '123 4th Street', 'Memphis', 'TN', '37544', 'USA', NULL, NULL, '');

INSERT INTO northwind.shippers (`id`, `company`, `last_name`, `first_name`, `email_address`, `job_title`, `business_phone`, `home_phone`, `mobile_phone`, `fax_number`, `address`, `city`, `state_province`, `zip_postal_code`, `country_region`, `web_page`, `notes`, `attachments`)
VALUES (5, 'Shipping Company E', 'Page', 'Jimmy', 'jpage@companye.com', 'Shipping Agent', '(901)656-0100', NULL, NULL, '(901)656-0101', '123 5th Street', 'Memphis', 'TN', '37544', 'USA', NULL, NULL, '');

-----------------------------------------------------
-- Verify the Changes Were Made
-- --------------------------------------------------
SELECT * FROM northwind.shippers
WHERE id >= 2;

SELECT * FROM northwind_dw2.dim_shippers
WHERE shipper_id >= 2;