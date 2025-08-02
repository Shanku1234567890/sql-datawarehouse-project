select * from silver.crm_cust_info
select * from silver.erp_cust_az12

create view gold.dim_customers as
select 
row_number()over(order by cst_id) as [key number],
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_create_date,
bdate,
case when cst_gndr!='N/A' then cst_gndr
else  coalesce(gen,'N/A')
END NEW_GEN,
cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key=la.cid

select*from gold.dim_customers

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
