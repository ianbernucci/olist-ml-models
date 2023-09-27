WITH base_pedido AS( 
  SELECT a.idPedido,
    b.idVendedor,
    a.descSituacao,
    a.dtPedido,
    a.dtAprovado,
    a.dtEntregue,
    a.dtEstimativaEntrega,
    SUM(vlFrete) AS total_frete

  FROM silver.olist.pedido AS a

  LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

  WHERE a.dtPedido < '{date}'
    AND a.dtPedido >= add_months('{date}', -6)
    AND idVendedor IS NOT NULL

  GROUP BY a.idPedido,
  b.idVendedor,
  a.descSituacao,
  a.dtPedido,
  a.dtAprovado,
  a.dtEntregue,
  a.dtEstimativaEntrega)


  SELECT idVendedor,
    COUNT(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue, '{date}')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS perc_pedido_atrasado,
    COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS perc_pedido_cancelado,
    AVG(total_frete) AS media_frete,
    PERCENTILE(total_frete, 0.5) AS mediana_frete,
    MIN(total_frete) AS min_frete,
    MAX(total_frete) AS max_frete,
    AVG(DATEDIFF(coalesce(dtEntregue, '{date}'), dtAprovado)) AS qtd_dias_aprovado_entrega,
    AVG(DATEDIFF(coalesce(dtEntregue, '{date}'), dtPedido)) AS qtd_dias_pedido_entrega,
    AVG(DATEDIFF(dtEstimativaEntrega, coalesce(dtEntregue, '{date}'))) AS qtd_dias_entrega_promessa,
    '{date}' AS data_referencia,
    NOW() AS dtIngestion

  FROM base_pedido

  GROUP BY idVendedor, data_referencia, dtIngestion


