SELECT
    o.order_id,
    o.total_amount,
    SUM(i.amount) AS items_total
FROM {{ ref('fact_orders') }} o
JOIN {{ ref('fact_order_items') }} i ON o.order_id = i.order_id
GROUP BY o.order_id, o.total_amount
HAVING ABS(o.total_amount - SUM(i.amount)) > 0.01
