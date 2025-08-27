-- --------------------------------------------------------------------------------------------------------------
-- TODO: Extract the appropriate data from the northwind database, and INSERT it into the Northwind_DW database.
-- --------------------------------------------------------------------------------------------------------------
USE northwind_dw;
-- ----------------------------------------------
-- Populate dim_customers
-- ----------------------------------------------
INSERT INTO `northwind_dw`.`dim_customers`
(`customer_key`,
`company`,
`last_name`,
`first_name`,
`job_title`,
`business_phone`,
`fax_number`,
`address`,
`city`,
`state_province`,
`zip_postal_code`,
`country_region`)
SELECT `id`,
`company`,
`last_name`,
`first_name`,
`job_title`,
`business_phone`,
`fax_number`,
`address`,
`city`,
`state_province`,
`zip_postal_code`,
`country_region`
FROM northwind.customers;

-- ----------------------------------------------
-- Validate that the Data was Inserted ----------
-- ----------------------------------------------
SELECT * FROM northwind_dw.dim_customers;


-- ----------------------------------------------
-- Populate dim_employees
-- ----------------------------------------------
INSERT INTO `northwind_dw`.`dim_employees`
(`employee_key`,
`company`,
`last_name`,
`first_name`,
`email_address`,
`job_title`,
`business_phone`,
`home_phone`,
`fax_number`,
`address`,
`city`,
`state_province`,
`zip_postal_code`,
`country_region`,
`web_page`)
SELECT `employees`.`id`,
    `employees`.`company`,
    `employees`.`last_name`,
    `employees`.`first_name`,
    `employees`.`email_address`,
    `employees`.`job_title`,
    `employees`.`business_phone`,
    `employees`.`home_phone`,
    `employees`.`fax_number`,
    `employees`.`address`,
    `employees`.`city`,
    `employees`.`state_province`,
    `employees`.`zip_postal_code`,
    `employees`.`country_region`,
    `employees`.`web_page`
FROM `northwind`.`employees`;

-- ----------------------------------------------
-- Validate that the Data was Inserted ----------
-- ----------------------------------------------
SELECT * FROM northwind_dw.dim_employees;


-- ----------------------------------------------
-- Populate dim_products
-- ----------------------------------------------
INSERT INTO `northwind_dw`.`dim_products`
(`product_key`,
`product_code`,
`product_name`,
`standard_cost`,
`list_price`,
`reorder_level`,
`target_level`,
`quantity_per_unit`,
`discontinued`,
`minimum_reorder_quantity`,
`category`)
# TODO: Write a SELECT Statement to Populate the table;
SELECT 
    `products`.`id`,
    `products`.`product_code`,
    `products`.`product_name`,
    `products`.`standard_cost`,
    `products`.`list_price`,
    `products`.`reorder_level`,
    `products`.`target_level`,
    `products`.`quantity_per_unit`,
    `products`.`discontinued`,
    `products`.`minimum_reorder_quantity`,
    `products`.`category`
FROM `northwind`.`products`;
-- ----------------------------------------------
-- Validate that the Data was Inserted ----------
-- ----------------------------------------------
SELECT * FROM northwind_dw.dim_products;


-- ----------------------------------------------
-- Populate dim_shippers
-- ----------------------------------------------
INSERT INTO `northwind_dw`.`dim_shippers`
(`shipper_key`,
`company`,
`address`,
`city`,
`state_province`,
`zip_postal_code`,
`country_region`)
# TODO: Write a SELECT Statement to Populate the table;
SELECT `shippers`.`id`,
    `shippers`.`company`,
    `shippers`.`address`,
    `shippers`.`city`,
    `shippers`.`state_province`,
    `shippers`.`zip_postal_code`,
    `shippers`.`country_region`
FROM `northwind`.`shippers`;

-- ----------------------------------------------
-- Validate that the Data was Inserted ----------
-- ----------------------------------------------
SELECT * FROM northwind_dw.dim_shippers;



-- ----------------------------------------------
-- Populate fact_orders
-- ----------------------------------------------
INSERT INTO `northwind_dw`.`fact_orders`
(`order_key`,
`product_key`,
`employee_key`,
`customer_key`,
`shipper_key`,
`quantity`,
`unit_price`,
`discount`,
`date_allocated`,
`order_date`,
`shipped_date`,
`taxes`,
`payment_type`,
`paid_date`,
`tax_rate`,
`orders_status_name`,
`order_details_status_name`)
Select o.id, 
   od.product_id,
   o.employee_id,
    o.customer_id,
    o.shipper_id,
    od.quantity,
    od.unit_price,
    od.discount,
    od.date_allocated,
    o.order_date,
    o.shipped_date,
    o.taxes,
    o.payment_type,
    o.paid_date,
    o.tax_rate,
 os.status_name as order_status 
,ods.status_name as order_details_status
from northwind.orders as o 
inner join northwind.orders_status as os
on o.status_id = os.id
inner join northwind.order_details as od
on o.id = od.order_id 
inner join northwind.order_details_status as ods
on od.status_id = ods.id;

/* 
--------------------------------------------------------------------------------------------------
TODO: Write a SELECT Statement that:
- JOINS the northwind.orders table with the northwind.orders_status table
- JOINS the northwind.orders with the northwind.order_details table.
--  (TIP: Remember that there is a one-to-many relationship between orders and order_details).
- JOINS the northwind.order_details table with the northwind.order_details_status table.
--------------------------------------------------------------------------------------------------
- The column list I've included in the INSERT INTO clause above should be your guide to which 
- columns you're required to extract from each of the four tables. Pay close attention!
--------------------------------------------------------------------------------------------------
*/


-- ----------------------------------------------
-- Validate that the Data was Inserted ----------
-- ----------------------------------------------
SELECT * FROM northwind_dw.fact_orders;