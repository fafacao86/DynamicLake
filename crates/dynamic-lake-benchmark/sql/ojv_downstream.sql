SELECT paimon.analytics.mv.id AS id,
       paimon.analytics.mv.b_id AS b_id
FROM paimon.analytics.mv
LEFT JOIN paimon.analytics.a
  ON paimon.analytics.mv.id = paimon.analytics.a.customer_key
