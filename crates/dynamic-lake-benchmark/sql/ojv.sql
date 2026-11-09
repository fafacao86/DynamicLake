SELECT paimon.analytics.a.customer_key AS id,
       paimon.analytics.b.order_customer_key AS b_id
FROM paimon.analytics.a
LEFT JOIN paimon.analytics.b
  ON paimon.analytics.a.customer_key = paimon.analytics.b.order_customer_key
