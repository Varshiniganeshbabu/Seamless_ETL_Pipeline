-- Query processed student orders
SELECT * FROM orders_processed LIMIT 10;

-- Count of each product
SELECT product, COUNT(*) AS total_orders
FROM orders_processed
GROUP BY product
ORDER BY total_orders DESC;
