--EXPLORATORY DATA ANALYSIS PROJECT

/*

==============================================================================================================

Exploratory Data Analysis project

===============================================================================================================

This project mainly focusses on the varopus data explorations by joining fact tables into dimensions

	Initialization

		1. Database named Datawarehouseanalytics creation
		2. Schema named gold creation within Datawarehouseanalytics
		3. Table creation and insertion using bulk insert from Datawarehouse gold layer view
			gold.dim_products
			gold.dim_customers
			gold.fact_sales

	Exploratory Data Analysis

		This includes various EDA methods and techniques which standard industry follows.

			DATABASE EXPLORATION
			DIMENSION EXPLORATION
			MEASURE EXPLORATION
			MAGNITUDE CHECK
			DATE EXPLORATION
			RANKING


*/
--Initialization

USE master;

--Create Database Datawarehouseanalytics

IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'Datawarehouseanalytics')

BEGIN
	ALTER DATABASE Datawarehouseanalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouseanalytics;
	CREATE DATABASE DataWarehouseAnalytics;
END;

USE DataWarehouseAnalytics;

--SCHEMA gold creation

IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
	DROP SCHEMA gold
END


CREATE SCHEMA gold;

--tables gold.dim_products,gold.dim_customers,gold.fact_sales creation

IF EXISTS ( SELECT 1 FROM sys.tables WHERE name = 'gold.dim_products')
BEGIN
DROP TABLE gold.dim_customers
END

CREATE TABLE gold.dim_customers(
customer_key BIGINT,
customer_id INT,
customer_number VARCHAR(50),
first_name VARCHAR(50),
last_name VARCHAR(50),
country VARCHAR(50),
marital_status VARCHAR(50),
gender VARCHAR(50),
birthdate VARCHAR(50),
create_date DATE 
)

IF EXISTS ( SELECT 1 FROM sys.tables WHERE name = 'gold.dim_products')
BEGIN
	DROP TABLE gold.dim_products
END

CREATE TABLE gold.dim_products (
product_key BIGINT,
product_id INT,
product_number VARCHAR(50),
product_name VARCHAR(50),
category_id VARCHAR(50),
category VARCHAR(50),
subcategory VARCHAR(50),
maintenance VARCHAR(50),
cost VARCHAR(50),
product_line VARCHAR(50),
start_date VARCHAR(50)
)

IF EXISTS ( SELECT 1 FROM sys.tables WHERE name ='gold.fact_sales')
BEGIN
DROP TABLE gold.fact_sales
END

CREATE TABLE gold.fact_sales(
order_number VARCHAR(50),
product_key BIGINT,
customer_key BIGINT,
order_date DATE,
ship_date DATE,
due_date DATE,
sales INT,
quantity INT,
price INT
)

TRUNCATE TABLE gold.dim_customers

GO
BULK INSERT gold.dim_customers
FROM 'C:\SQL\Baara\Data_Analytics_Project\DataAnalytics_Project_2\gold.dim_customers.csv'
WITH (
FIRSTROW =1,
FIELDTERMINATOR =',',
TABLOCK
)

SELECT TOP 1 *
FROM gold.dim_customers;

TRUNCATE TABLE gold.dim_products
BULK INSERT gold.dim_products
FROM 'C:\SQL\Baara\Data_Analytics_Project\DataAnalytics_Project_2\gold.dim_products.csv'
WITH(
FIRSTROW = 1,
FIELDTERMINATOR = ',',
TABLOCK
);
USE DatawarehouseAnalytics;
SELECT TOP 1 *
FROM gold.dim_products;

SELECT 
TOP 1 *
FROM gold.dim_customers;

USE Datawarehouseanalytics;

TRUNCATE TABLE gold.fact_sales;
BULK INSERT gold.fact_sales
FROM 'C:\SQL\Baara\Data_Analytics_Project\DataAnalytics_Project_2\gold.fact_sales.csv'
WITH (
FIRSTROW =1,
FIELDTERMINATOR = ',',
TABLOCK
);


--Exploratory Data Analysis Process

--Database Exploration


SELECT
*
FROM INFORMATION_SCHEMA.TABLES;

SELECT
* 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';

--Dimension & Fact Exploration

SELECT TOP 1 *
FROM gold.dim_customers;

SELECT
DISTINCT country
FROM gold.dim_customers;

SELECT
TOP 1 *
FROM gold.dim_products;

SELECT
DISTINCT category
FROM gold.dim_products;

SELECT
DISTINCT subcategory
FROM gold.dim_products;

--Measure Exploration


SELECT
*
FROM
(
SELECT
DISTINCT category,subcategory,COUNT(subcategory) AS subcategory_count
FROM gold.dim_products
GROUP BY category,subcategory

)T
WHERE category IS NOT NULL
ORDER BY category ;


SELECT
TOP 1 *
FROM gold.dim_customers;

SELECT
TOP 1 *
FROM gold.dim_products;

SELECT
TOP 1 *
FROM gold.fact_sales;

SELECT
p.product_id,
p.product_number,
p.category_id,
p.category,
p.subcategory,
MIN(s.order_date) AS first_order_date,
s.order_date
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key = s.product_key
WHERE s.order_date IS NOT NULL
GROUP BY p.product_id,
p.product_number,
p.category_id,
p.category,
p.subcategory,
s.order_date
--HAVING  s.order_date IS NOT NULL

SELECT
*
FROM
(
SELECT
p.product_id,
p.product_name,
p.category,
p.subcategory,
MIN(s.order_date) AS min_order_date,
MAX(s.order_date) AS max_order_date,
DATEDIFF(DAY,MIN(s.order_date),MAX(s.order_date)) AS date_difference
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key = s.product_key
GROUP BY p.product_id,
p.product_name,
p.category,
p.subcategory
)T
WHERE date_difference IS NOT NULL
ORDER BY date_difference DESC


--FIND THE TOTAL SALES

SELECT 
TOP 1 *
FROM gold.fact_sales;

SELECT SUM(sales) AS Total_sales
FROM gold.fact_sales;

--how many items are sold
SELECT 'Total_Sales' AS 'Measured_Item', SUM(sales) AS 'Measured_Quantity'
FROM gold.fact_sales
UNION

SELECT 'Total_Quantity' as 'Measured_Item',COUNT(quantity) AS 'Measured_Quantity'
FROM gold.fact_sales
UNION
SELECT 'Avg_Price' AS 'Measured_Item', AVG(price) AS 'Measured_Quantity'
FROM gold.fact_sales
UNION
SELECT 'Total_Orders' AS 'Measured_Item', COUNT(DISTINCT order_number) AS 'Measured_Quantity'
FROM gold.fact_sales
UNION
SELECT 'Total_Products' AS 'Measured_Item', COUNT(DISTINCT product_id) AS 'Measured_Quantity'
FROM gold.dim_products
UNION
SELECT 'Total_Customers' AS 'Measured_Item', COUNT(DISTINCT customer_id) AS 'Measured_Quantity'
FROM gold.dim_customers
UNION
SELECT 'Customer_Orders' AS 'Measured_Item', COUNT(DISTINCT customer_key) AS 'Measured_Quantity'
FROM gold.fact_sales;

-- Magnitude Check Exploration

--Total number of customers by country

SELECT
country,
COUNT(customer_id) AS Customer_Count
FROM gold.dim_customers
GROUP BY country;
--Total number of customers by gender

SELECT
gender,
COUNT(customer_id) AS customer_count
FROM gold.dim_customers
GROUP BY gender
--Total number of products by category

SELECT
category,
COUNT( product_key) AS product_count
FROM gold.dim_products
GROUP BY category
--What is the average cost in each category

SELECT p.category,
AVG(s.price) AS Avg_price
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key  = s.product_key
GROUP BY p.category
--What is the total revenue generated in each category

SELECT p.category,
SUM(s.price) AS Total_Revenue
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key  = s.product_key
GROUP BY p.category
--Find the total revenue generated by each customer

SELECT
customer_key,
SUM(sales) AS Total_Rev_Per_Customer
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY customer_key;

--What is the distribution of sold items across countries

SELECT
c.country,
COUNT(s.product_key) AS Product_Count
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
ON s.customer_key = c.customer_key
GROUP BY c.country

SELECT 'Total_Customers' AS 'Measured_Item', COUNT(DISTINCT customer_id) AS 'Measured_Quantity' FROM gold.dim_customers
UNION
SELECT 'Customers_by_gender' AS 'Measured_Item', COUNT(DISTINCT customer_id) AS 'Measured_Quantity' FROM gold.dim_customers
UNION
SELECT 'Total_Product_Count' AS 'Measured_Item', COUNT(product_id) AS 'Measured_Quantity' FROM gold.dim_products;

SELECT TOP 1 *
FROM gold.fact_sales;

SELECT TOP 1 *
FROM gold.dim_products;



SELECT
TOP 1 *
FROM gold.dim_customers;

SELECT
country,
COUNT(DISTINCT customer_id) AS customer_count
FROM gold.dim_customers
GROUP BY country;

SELECT gender,
COUNT(gender) AS customer_count
FROM gold.dim_customers
GROUP BY gender;

SELECT
TOP 1 *
FROM gold.dim_products;

SELECT category,
COUNT(DISTINCT product_id) AS product_count
FROM gold.dim_products
GROUP BY category;


--Ranking Analysis

--Top 5 products with highest revenue

SELECT
TOP 1 *
FROM gold.fact_sales;

SELECT TOP 1 *
FROM gold.dim_products;

SELECT 
TOP 5 p.product_name,
SUM(s.sales) AS Total_Sales,
RANK() OVER ( ORDER BY SUM(s.sales) DESC) AS product_rank
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key = s.product_key
GROUP BY product_name;


--top 5 worst performing products


SELECT
*
FROM gold.fact_sales
WHERE product_key = 175
SELECT 
TOP 5 p.product_name,
SUM(s.sales) AS Total_Sales,
RANK() OVER ( ORDER BY SUM(s.sales)) AS product_rank
FROM gold.dim_products p
LEFT JOIN gold.fact_sales s
ON p.product_key = s.product_key
GROUP BY product_name
HAVING SUM(s.sales) >=0;

SELECT
TOP 10
customer_key,
SUM(sales) AS Total_Sales,
RANK() OVER( ORDER BY SUM(sales) DESC) AS customer_rank
FROM gold.fact_sales
GROUP BY customer_key


SELECT *
FROM
(
SELECT
customer_key,
COUNT(DISTINCT order_number) AS Order_Count,
RANK() OVER( ORDER BY COUNT(DISTINCT order_number) ASC) AS customer_rank
FROM gold.fact_sales
GROUP BY customer_key
)T
WHERE customer_rank <=3