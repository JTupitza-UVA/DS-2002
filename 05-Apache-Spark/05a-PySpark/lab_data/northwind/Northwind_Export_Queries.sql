use northwind;

# -------------------------------------------------------
# JSON Files
# -------------------------------------------------------
SELECT * FROM northwind.customers;
SELECT * FROM northwind.invoices;
SELECT id
	, company
    , last_name
    , first_name
    , job_title
FROM northwind.suppliers;


# -------------------------------------------------------
# CSV Files
# -------------------------------------------------------
SELECT * FROM northwind.employees;
SELECT id
	, company
    , address
    , city
    , state_province
    , zip_postal_code
    , country_region
FROM northwind.shippers;


# -------------------------------------------------------
# ORDERS - 58 Rows : Batches of 20, 19 and 19
# -------------------------------------------------------
SELECT o.id AS order_id,
	od.id AS order_detail_id,
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
ON od.status_id = ods.id
# WHERE od.id BETWEEN 27 AND 46;
# WHERE od.id BETWEEN 47 AND 69;
WHERE od.id > 69;

# -------------------------------------------------------
# INVENTORY TRANSACTIONS - 102 rows: 34 per batch
# -------------------------------------------------------
SELECT it.id AS inventory_transaction_id
    , itt.type_name AS transaction_type
	, it.transaction_created_date
	, it.transaction_modified_date
    #, it.transaction_type AS transaction_type_id
    , it.product_id
    , it.quantity
FROM northwind.inventory_transactions AS it
INNER JOIN northwind.inventory_transaction_types AS itt
ON it.transaction_type = itt.id
# WHERE it.id < 69
# WHERE it.id BETWEEN 69 AND 102
WHERE it.id > 102
ORDER BY it.id ASC;


# -------------------------------------------------------
# PURCHASE ORDERS - 55 rows: batches of 19, 18 and 18
# -------------------------------------------------------
SELECT po.id AS purchase_order_id
	, pod.id AS purchase_order_detail_id
	, pod.product_id
	, po.supplier_id
    , pod.inventory_id
    , po.created_by
    , po.approved_by
    , po.submitted_by
    , po.submitted_date
    , po.creation_date
    , po.approved_date
    , pod.date_received
    , pod.quantity
    , pod.unit_cost
    , po.shipping_fee
    , po.taxes
    , po.payment_amount
    , po.payment_method
    , pod.posted_to_inventory
    , pos.status AS purchase_order_status
FROM purchase_orders AS po
INNER JOIN purchase_order_status AS pos
ON pos.id = po.status_id
INNER JOIN purchase_order_details AS pod
ON po.id = pod.purchase_order_id
# WHERE pod.id BETWEEN 238 AND 256
# WHERE pod.id BETWEEN 257 AND 274
WHERE pod.id BETWEEN 275 AND 295
ORDER BY pod.id ASC;

SELECT * FROM purchase_order_details;









