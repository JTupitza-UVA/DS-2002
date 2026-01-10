-- --------------------------------------------------------------------------------------
-- Create Stored Procedures that Implement Type 2 Slowly-Changing Dimensions
-- --------------------------------------------------------------------------------------
USE northwind_dw;

-- -------------------------------------------------------------------
-- Customers Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_customers;

CREATE PROCEDURE merge_customers(
	IN cust_key INT,
    IN customer_id INT,
    IN company VARCHAR(50),
    IN last_name VARCHAR(50),
    IN first_name VARCHAR(50),
    IN job_title VARCHAR(50),
    IN business_phone VARCHAR(25),
    IN fax_number VARCHAR(25),
    IN address LONGTEXT,
    IN city VARCHAR(50),
    IN state_province VARCHAR(50),
    IN zip_postal_code VARCHAR(15),
    IN country_region VARCHAR(50)
)
BEGIN
    IF EXISTS (SELECT 1 FROM dim_customers WHERE customer_key = cust_key) THEN
        UPDATE northwind_dw.dim_customers
        SET customer_id = customer_id,
			company = company,
			last_name = last_name,
			first_name = first_name,
			job_title = job_title,
			business_phone = business_phone,
			fax_number = fax_number,
			address = address,
			city = city,
			state_province = state_province,
			zip_postal_code = zip_postal_code,
			country_region = country_region
        WHERE customer_key = cust_key;
    ELSE
        INSERT INTO northwind_dw.dim_customers (
			customer_id
            , company
            , last_name
            , first_name
            , job_title
            , business_phone
            , fax_number
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region)
        VALUES (
			customer_id
            , company
            , last_name
            , first_name
            , job_title
            , business_phone
            , fax_number
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region);
    END IF;
END$$

DELIMITER ;

-- ---------------------------------------------------------------------
-- Unit Test 'merge_customers'
-- ---------------------------------------------------------------------
-- Changes Area Code and 'Zip_Postal_Code' to accurate values.
CALL merge_customers(29, 29, 'Company CC', 'Lee', 'Soo Jung', 'Purchasing Manager', '(123)555-0100', '(123)555-0101', '789 29th Street', 'Denver', 'CO', '99999', 'USA');
-- Creates a New Customer Record
CALL merge_customers(30, 30, 'Company DD', 'McCartney', 'Paul', 'Purchasing Manager', '(703)555-1234', '(703)555-1212', '258 30th Street', 'Alexandria', 'VA', '22314', 'USA');


-- View the Results ----------------------    
SELECT * FROM northwind_dw.dim_customers
WHERE customer_key >= 29;

-- -------------------------------------------------------------------
-- Employees Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_employees;

CREATE PROCEDURE merge_employees(
	IN emp_key INT,
    IN employee_id INT,
    IN company VARCHAR(50),
    IN last_name VARCHAR(50),
    IN first_name VARCHAR(50),
    IN email_address VARCHAR(50),
    IN job_title VARCHAR(50),
    IN business_phone VARCHAR(25),
    IN home_phone VARCHAR(25),
    IN fax_number VARCHAR(25),
    IN address LONGTEXT,
    IN city VARCHAR(50),
    IN state_province VARCHAR(50),
    IN zip_postal_code VARCHAR(15),
    IN country_region VARCHAR(50),
    IN web_page LONGTEXT
)
BEGIN
    IF EXISTS (SELECT 1 FROM dim_employees WHERE employee_key = emp_key) THEN
        UPDATE northwind_dw.dim_employees
        SET employee_id = employee_id,
			company = company,
			last_name = last_name,
			first_name = first_name,
            email_address = email_address,
			job_title = job_title,
			business_phone = business_phone,
            home_phone = home_phone,
			fax_number = fax_number,
			address = address,
			city = city,
			state_province = state_province,
			zip_postal_code = zip_postal_code,
			country_region = country_region,
            web_page = web_page
        WHERE employee_key = emp_key;
    ELSE
        INSERT INTO northwind_dw.dim_employees (
			employee_id
            , company
            , last_name
            , first_name
            , email_address
            , job_title
            , business_phone
            , home_phone
            , fax_number
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region
            , web_page)
        VALUES (
			employee_id
            , company
            , last_name
            , first_name
            , email_address
            , job_title
            , business_phone
            , home_phone
            , fax_number
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region
            , web_page);
    END IF;
END$$

DELIMITER ;

-- ---------------------------------------------------------------------
-- Unit Test 'merge_employees'
-- ---------------------------------------------------------------------
-- Changes Area Code and 'Zip_Postal_Code' to accurate values.
CALL merge_employees(9, 9, 'Northwind Traders', 'Hellung-Larsen', 'Anne', 'anne@northwindtraders.com', 'Sales Representative', '(206)555-0100', '(206)555-0102', '(206)555-0103', '123 9th Avenue', 'Seattle', 'WA', '98104', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#');
-- Creates a New Customer Record
CALL merge_employees(10, 10, 'Northwind Traders', 'McCartney', 'Paul', 'paul@northwindtraders.com', 'Purchasing Manager', '(206)555-1234', '(206)555-1212', '(206)555-4321', '258 30th Street', 'Seattle', 'WA', '98104', 'USA', 'http://northwindtraders.com#http://northwindtraders.com/#');


-- View the Results ----------------------    
SELECT * FROM northwind_dw.dim_employees
WHERE employee_key >= 9;


-- -------------------------------------------------------------------
-- TODO: Products Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_products;

CREATE PROCEDURE merge_products(

)
BEGIN

END$$

DELIMITER ;

-- ---------------------------------------------------------------------
-- Unit Test 'merge_products'
-- ---------------------------------------------------------------------
-- Changes Area Code and 'Zip_Postal_Code' to accurate values.
CALL merge_products(45, 99, 'NWTSO-99', 'Northwind Traders Chicken Soup', '1.0000', '1.9500', '100', '200', '18.5 oz', '0', 50, 'Soups');
-- Creates a New Customer Record
CALL merge_products(46, 100, 'NWTSO-100', 'Northwind Traders Beef Soup', '1.0000', '1.9500', '100', '200', '18.5 oz', '0', 50, 'Soups');


-- View the Results ----------------------    
SELECT * FROM northwind_dw.dim_products
WHERE product_key >= 45;



-- -------------------------------------------------------------------
-- TODO: Shippers Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_shippers;

CREATE PROCEDURE merge_shippers(

)
BEGIN

END$$

DELIMITER ;

-- ---------------------------------------------------------------------
-- Unit Test 'merge_shippers'
-- ---------------------------------------------------------------------
-- Changes Area Code and 'Zip_Postal_Code' to accurate values.
CALL merge_shippers(3, 3, 'Shipping Company C', '123 Any Street', 'Memphis', 'TN', '38103', 'USA');
-- Creates a New Customer Record
CALL merge_shippers(4, 4, 'Shipping Company C', '123 Any Street', 'Memphis', 'TN', '38103', 'USA');


-- View the Results ----------------------    
SELECT * FROM northwind_dw.dim_shippers
WHERE shipper_key >= 3;