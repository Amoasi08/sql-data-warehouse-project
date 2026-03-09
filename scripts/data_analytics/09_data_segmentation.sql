/*Segment products into cost ranges and 
count how many products fall into each segment*/
WITH products_cost_ranges AS(
SELECT
	product_name,
	cost,
	CASE WHEN cost < 100 THEN 'Below 100'
		 WHEN cost >= 100 AND cost <= 500 THEN '100-500'
		 WHEN cost > 500 AND cost <= 1000 THEN '500-1000'
		 ELSE 'Above 1000'
	END 'cost_range'
FROM gold.dim_products
)

SELECT
cost_range,
COUNT(product_name) AS total_products
FROM products_cost_ranges
GROUP BY cost_range
ORDER BY total_products DESC

------------------------------------------------------------
/* Example 2:
Group customers into three segments based on their spending behavior:
	-VIP: Customers with at least 12 months of history and spending more than 5,000 Euro.
	-Regular: Customers with at least 12 months of history but spending 5,000 Euros or less.
	-New: Customer with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS(
SELECT 
	c.customer_key,
	SUM(f.sales_amount) AS total_spending,
	MIN(f.order_date) AS first_order,
	MAX(f.order_date) AS last_order,
	DATEDIFF(MONTH,MIN(f.order_date),MAX(f.order_date)) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)

SELECT
	spending_customer_groups,
	COUNT(customer_key) AS total_customers
FROM
(
SELECT 
	customer_key,
	total_spending,
	lifespan,
	CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'NEW'
	END spending_customer_groups
FROM customer_spending)t
GROUP BY spending_customer_groups
ORDER BY total_customers DESC
