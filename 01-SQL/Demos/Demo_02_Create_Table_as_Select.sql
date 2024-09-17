CREATE DATABASE `Northwind_DW_Demo` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE Northwind_DW_Demo;

CREATE TABLE fact_orders AS
SELECT o.id,
	od.id,
    o.customer_id,
    o.employee_id,
    od.product_id,
    o.shipper_id,
    o.order_date,
    o.paid_date,
    o.shipped_date,
    o.payment_type,
    od.quantity,
    od.unit_price,
    od.discount,
    o.shipping_fee,
    o.taxes,
    o.tax_rate,
    os.status_name AS order_status,
    ods.status_name AS order_details_status
FROM northwind.orders AS o
INNER JOIN northwind.orders_status AS os
ON o.status_id = os.id
LEFT OUTER JOIN northwind.order_details AS od
ON o.id = od.order_id
INNER JOIN northwind.order_details_status AS ods
ON od.status_id = ods.id;


SELECT * FROM Northwind_DW_Demo.fact_orders;