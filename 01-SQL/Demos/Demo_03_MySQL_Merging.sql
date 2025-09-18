-- ---------------------------------------------------------------------
-- MySQL Does NOT Support the MERGE Statement, but it does provide two 
-- language constructs that can provide similar behavior. The first of
-- these uses "ON DUPLICATE KEY UPDATE" to modify an INSERT statement.
-- ---------------------------------------------------------------------
USE northwind_dw;

SELECT * FROM northwind_dw.dim_customers
WHERE customer_key = 29;

INSERT INTO northwind_dw.dim_customers (
	customer_key
	, customer_id
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
VALUES (29
    , 29
	, 'Company CC'
	, 'Lee'
	, 'Soo Jung'
	, 'Purchasing Manager'
	, '(123)555-0100'
	, '(123)555-0101'
	, '789 29th Street'
	, 'Denver'
	, 'CO'
	, '99999'
	, 'USA')
ON DUPLICATE KEY UPDATE customer_id = 29
	, company = 'Company CC'
	, last_name = 'Lee'
	, first_name = 'Soo Jung'
	, job_title = 'Purchasing Manager'
	, business_phone = '(303)555-0100'
	, fax_number = '(303)555-0101'
	, address = '789 29th Street'
	, city = 'Denver'
	, state_province = 'CO'
	, zip_postal_code = '80033'
	, country_region = 'USA';

-- Unit Test the Results ----------------------    
SELECT * FROM northwind_dw.dim_customers
WHERE customer_key = 29;
    
-- ---------------------------------------------------------------------
-- The second construct involves creating a Stored Procedure that
-- simultaneously Updates Existing Rows while Inserting New Rows. 
-- ---------------------------------------------------------------------    
    
DELIMITER $

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
        WHERE customer_key = customer_key;
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
-- Call (Invoke) the New Stored Procedure
-- ---------------------------------------------------------------------
-- Changes Area Code and 'Zip_Postal_Code' to accurate values.
CALL merge_customers(29, 29, 'Company CC', 'Lee', 'Soo Jung', 'Purchasing Manager', '(123)555-0100', '(123)555-0101', '789 29th Street', 'Denver', 'CO', '99999', 'USA');
-- Creates a New Customer Record
CALL merge_customers(30, 30, 'Company DD', 'McCartney', 'Paul', 'Purchasing Manager', '(703)555-1234', '(703)555-1212', '258 30th Street', 'Alexandria', 'VA' '22314', 'USA');


-- Unit Test the Results ----------------------    
SELECT * FROM northwind_dw.dim_customers
WHERE customer_key IN (29, 30);