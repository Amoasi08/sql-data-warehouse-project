--(Trends) Analyze how a measure evolves over time
SELECT 
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_Sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)

----------------------------------------------------------
SELECT 
	DATETRUNC(month, order_date) AS order_date, --Truncate the date to the month
	SUM(sales_amount) AS total_Sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date)

----------------------------------------------------------
--changes over the years
SELECT 
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_Sales,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL 
GROUP BY YEAR(order_date)
