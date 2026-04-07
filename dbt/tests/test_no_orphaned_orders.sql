SELECT
    o.order_id
FROM {{ ref('fact_orders') }} o
LEFT JOIN {{ ref('fact_order_items') }} i ON o.order_id = i.order_id
WHERE i.item_id IS NULL
