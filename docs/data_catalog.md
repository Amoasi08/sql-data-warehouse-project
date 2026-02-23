## Data Dictionary for Gold Layer

### Overview
The Gold Layer is the business level data representation structured to support analytical and reporting use cases.
It consists of **dimension tables** and **fact tables** for specific business metrics.
#
### 1. gold.dim_customers
- **Purpose:** Stores customer detail enriched with demographic and geographic data
- **Columns:**

| Column Name | Data Type | Description |
|:--:|:--:|:--:|
|column_key |INT |Surrogate key uniquely identifying each customer records in the dimension table.|
|column_id |INT |Unique numerical identifier assigned to each customer|
|column_number |NVARCHAR(50) |Alphanumeric idenfier representing the customer,used for tracking and referencing.|
|first_name |NVARCHAR(50) |The customer's first name as recorded in the system.|
|last_name |NVARCHAR(50) |The customer's last name or family name.|
|country |NVARCHAR(50) |The country of residence for the customer(eg. 'Australia').|
|marital_status |NVARCHAR(50) |The marital status of the customer(eg. 'Married', 'Single')|
|gender |NVARCHAR(50) |The gender of the customer (eg. 'Male','Female','n/a') |
|birthday |DATE |The date of birth of the customer, formated as YYYY-MM-DD (eg. 1971-10-06). |
|create_date |DATE |The date and time when the customer record was created in the system. |

### 2. gold.dim_products
- **Purpose:** Provides information about the products and their attributes
- **Columns:**
  
| Column Name | Data Type | Description |
|:--:|:--:|:--:|
|product_key |INT |Surrogate key uniquely identifying each product records in the dimension table.|
|product_id |INT |Unique numerical identifier assigned to each product|
|product_number |NVARCHAR(50) |Alphanumeric idenfier representing the product,used for tracking and referencing.|
|category_id |NVARCHAR(50) |Special Identifier used to assign each product to a category|
|category |NVARCHAR(50) |The category shows how the product is grouped.|
|subcategory |NVARCHAR(50) |The subcategory is the breaksdown of broad categories into smaller specifics where the products can be located.|
|maintenance|NVARCHAR(50) |Provides information whether the product is maintained(eg. 'Yes', 'No')|
|cost |NVARCHAR(50) |The cost of the product|
|product_line |NVARCHAR(50) |The product line provides specifics of how the products is to be made to suit the customer |
|start_date |DATE |The start date of the product |

### 2. gold.fact_sales
- **Purpose:** Provides verifiable information on the busniess events of the sales made 
- **Columns:**
  
| Column Name | Data Type | Description |
|:--:|:--:|:--:|
|order_number |INT |Unique alphanumeric identifier assigned to each sales|
|product_key |INT |Surrogate key uniquely identifying each product records in the dimension table.|
|column_key |INT |Surrogate key uniquely identifying each customer records in the dimension table.|
|order_date |DATE |The order date of the product by the customer |
|shipping_date |DATE |The shipping date of the product to the customer |
|due_date |DATE |The due date of the product to the customer |
|sales_amount |INT |The sales amount is the quantity of products needed by the price of the product.|
|quantity |INT |The quantity is the number of products.|
|price |INT |The price is the cost of a product.|
