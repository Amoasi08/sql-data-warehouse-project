/*
========================================================================
Product Report
========================================================================
Purpose:
	- This report consolidates key product metrics and behaviors.

Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculate valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
===========================================================================
*/
/*--------------------------------------------------------------
Creating Or Altering the View
--------------------------------------------------------------*/
CREATE OR ALTER VIEW gold.report_products AS

WITH base_query AS(
/*--------------------------------------------------------------
1) Base Query: Retrieve core columns from tables
--------------------------------------------------------------*/
SELECT
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.product_line,
	p.cost AS product_cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
)

,product_aggregation AS (
/*-----------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
-----------------------------------------------------------------------*/
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	product_line,
	product_cost,
	DATEDIFF(MONTH,MIN(order_date), MAX(order_date)) AS lifespan_months,
	MAX(order_date) AS last_sale_date,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity_purchased,
	ROUND(AVG(CAST(sales_amount AS FLOAT) /  NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query
GROUP BY 
	product_key,
	product_name,
	category,
	subcategory,
	product_line,
	product_cost
)

/*-----------------------------------------------------------------------
3) Final Query: Combines all product results into one output
-----------------------------------------------------------------------*/
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	product_line,
	product_cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS receny_in_months,
	CASE 
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan_months,
	total_orders,
	total_sales,
	total_quantity_purchased,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue (AOR)
	CASE
		WHEN total_sales = 0 THEN 0
		ELSE total_sales/total_orders
	END AS avg_order_value,
	-- Average Monthly Revenue
	CASE 
		WHEN lifespan_months = 0 THEN total_sales
		ELSE total_sales/ lifespan_months
	END AS avg_monthly_spend
FROM product_aggregation
