WITH base_pedido AS (
  SELECT DISTINCT
    a.idPedido,
    b.idVendedor

  FROM silver.olist.pedido AS a

  LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

  WHERE a.dtPedido < '{date}'
    AND a.dtPedido >= add_months('{date}', -6)
    AND b.idVendedor IS NOT NULL ),


base_join AS ( 
  SELECT a.*,
    b.vlNota

  FROM base_pedido AS a

  LEFT JOIN silver.olist.avaliacao_pedido AS b
    ON a.idPedido = b.idPedido),


base_resumo AS ( 
  SELECT idVendedor,
    AVG(vlNota) AS media_nota,
    PERCENTILE(vlNota, 0.5) AS mediana_nota,
    MIN(vlNota) AS min_nota,
    MAX(vlNota) AS max_nota,
    COUNT(vlNota) / COUNT(idPedido) AS perc_avaliacao

  FROM base_join

  GROUP BY idVendedor)


SELECT *,
  '{date}' AS data_referencia,
  NOW() AS dtIngestion

FROM base_resumo



