-- --------------------------------------------------------------------------------------
-- Create Stored Procedures that Implement Type 1 Slowly-Changing Dimensions
-- --------------------------------------------------------------------------------------
USE northwind_dw;

-- -------------------------------------------------------------------
-- Customers Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_customers;

CREATE PROCEDURE merge_customers(
	IN customer_key INT,
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
    IF EXISTS (SELECT 1 FROM dim_customers WHERE customer_key = customer_key) THEN
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

-- -------------------------------------------------------------------
-- Employees Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_employees;

CREATE PROCEDURE merge_employees(
	IN employee_key INT,
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
    IN country_region VARCHAR(50)
)
BEGIN
    IF EXISTS (SELECT 1 FROM dim_employees WHERE employee_key = employee_key) THEN
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
			country_region = country_region
        WHERE employee_key = employee_key;
    ELSE
        INSERT INTO northwind_dw.dim_employees (
			customer_id
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
            , country_region)
        VALUES (
			customer_id
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
            , country_region);
    END IF;
END$$

DELIMITER ;

-- -------------------------------------------------------------------
-- Products Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_products;

CREATE PROCEDURE merge_products(
	IN product_key INT,
    IN product_id INT,
    IN product_code VARCHAR(25),
    IN product_name VARCHAR(50),
    IN standard_cost DECIMAL(19,4),
    IN list_price DECIMAL(19,4),
    IN reorder_level INT,
    IN target_level INT,
    IN quantity_per_unit VARCHAR(50),
    IN discontinued TINYINT(1),
    IN minimum_reorder_quantity TINYINT(1),
    IN category VARCHAR(50)
)
BEGIN
    IF EXISTS (SELECT 1 FROM dim_products WHERE product_key = product_key) THEN
        UPDATE northwind_dw.dim_products
        SET product_id = product_id,
			product_code = product_code,
			product_name = product_name,
			standard_cost = standard_cost,
            list_price = list_price,
			reorder_level = reorder_level,
			target_level = target_level,
            quantity_per_unit = quantity_per_unit,
			discontinued = discontinued,
			minimum_reorder_quantity = minimum_reorder_quantity,
			category = category
        WHERE product_key = product_key;
    ELSE
        INSERT INTO northwind_dw.dim_products (
			product_id
            , product_code
            , product_name
            , standard_cost
            , list_price
            , reorder_level
            , target_level
            , quantity_per_unit
            , discontinued
            , minimum_reorder_quantity
            , category)
        VALUES (
			product_id
            , product_code
            , product_name
            , standard_cost
            , list_price
            , reorder_level
            , target_level
            , quantity_per_unit
            , discontinued
            , minimum_reorder_quantity
            , category);
    END IF;
END$$

DELIMITER ;

-- -------------------------------------------------------------------
-- Shippers Dimension
-- -------------------------------------------------------------------
DELIMITER $

DROP PROCEDURE IF EXISTS merge_shippers;

CREATE PROCEDURE merge_shippers(
	IN shipper_key INT,
    IN shipper_id INT,
    IN company VARCHAR(50),
    IN address LONGTEXT,
    IN city VARCHAR(50),
    IN state_province VARCHAR(50),
    IN zip_postal_code VARCHAR(15),
    IN country_region VARCHAR(50)
)
BEGIN
    IF EXISTS (SELECT 1 FROM dim_shippers WHERE shipper_key = shipper_key) THEN
        UPDATE northwind_dw.dim_shippers
        SET shipper_id = shipper_id,
			company = company,
			address = address,
			city = city,
            state_province = state_province,
			zip_postal_code = zip_postal_code,
			country_region = country_region
        WHERE shipper_key = shipper_key;
    ELSE
        INSERT INTO northwind_dw.dim_shippers (
			shipper_id
            , company
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region)
        VALUES (
			shipper_id
            , company
            , address
            , city
            , state_province
            , zip_postal_code
            , country_region);
    END IF;
END$$

DELIMITER ;