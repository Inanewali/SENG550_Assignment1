-- ==========================================
-- Part 4: SQL Queries
-- ==========================================

-- 1. List all customers with their contact details. (1 Mark)
SELECT customer_id, name, email, phone, address
FROM customers;

-- 2. Count the total number of orders in the database. (1 Mark)
SELECT COUNT(*) AS total_orders
FROM orders;

-- 3. Find all orders made by customer “Alice Johnson”. (2 Marks)
SELECT o.order_id, o.order_date, o.total_amount, o.product_name, o.product_category
FROM orders o
         JOIN customers c ON o.customer_id = c.customer_id
WHERE c.name = 'Alice Johnson';

-- 4. List all orders that have not yet been delivered (delivery status not “Delivered”). (2 Marks)
SELECT o.order_id, o.order_date, d.delivery_date, d.status
FROM orders o
         JOIN deliveries d ON o.order_id = d.order_id
WHERE d.status <> 'Delivered';

-- 5. Find the total amount spent by each customer, sorted from highest to lowest. (2 Marks)
SELECT c.name, SUM(o.total_amount) AS total_spent
FROM customers c
         JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.name
ORDER BY total_spent DESC;

-- 6. Find the number of orders per product category. (2 Marks)
SELECT product_category, COUNT(*) AS order_count
FROM orders
GROUP BY product_category
ORDER BY order_count DESC;

-- 7. Find customers who have placed more than 2 orders. (2 Marks)
SELECT c.name, COUNT(o.order_id) AS order_count
FROM customers c
         JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.name
HAVING COUNT(o.order_id) > 2;

-- 8. Find the product category with the highest total sales amount. (3 Marks)
SELECT product_category, SUM(total_amount) AS total_sales
FROM orders
GROUP BY product_category
ORDER BY total_sales DESC
LIMIT 1;
