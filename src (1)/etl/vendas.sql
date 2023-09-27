WITH base_pedido_item AS (
  SELECT b.*, a.dtPedido

  FROM silver.olist.pedido AS a

  LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

  WHERE a.dtPedido < '{date}'
    AND a.dtPedido >= add_months('{date}', -6)
    AND b.idVendedor IS NOT NULL),


base_resumo AS (
  SELECT idVendedor,
    COUNT(DISTINCT idPedido) AS qtd_pedidos,
    COUNT(DISTINCT date(dtPedido)) AS qtd_dias,
    COUNT(idProduto) AS qtd_itens,
    MIN(DATEDIFF('{date}', dtPedido)) AS qtd_recencia,
    SUM(vlPreco) / COUNT(DISTINCT idPedido) AS media_ticket,
    AVG(vlPReco) AS media_valor_produto,
    MAX(vlPReco) AS max_valor_produto,
    MIN(vlPReco) AS min_valor_produto,
    COUNT(idProduto) / COUNT(DISTINCT idPedido) AS media_produto_pedido

    FROM base_pedido_item
    GROUP BY idVendedor),


base_resumo_pedido AS(
  SELECT idVendedor,
    idPedido,
    SUM(vlPreco) AS valor_preco

  FROM base_pedido_item

  GROUP BY idVendedor, idPedido),


base_min_max AS (
  SELECT idVendedor,
    MIN(valor_preco) AS min_valor_pedido,
    MAX(valor_preco) AS max_valor_pedido
  FROM base_resumo_pedido

  GROUP BY idVendedor),


base_lifetime AS(
  SELECT b.idVendedor,
    SUM(vlPreco) AS LTV,
    MAX(DATEDIFF('{date}', dtPedido)) AS qtd_dias_base


  FROM silver.olist.pedido AS a

  LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

  WHERE a.dtPedido < '{date}'
    AND a.dtPedido >= add_months('{date}', -6)

  GROUP BY b.idVendedor),


base_data_pedido AS (
  SELECT DISTINCT idVendedor,
    DATE(dtPedido) AS dtPedido

  FROM base_pedido_item

  GROUP BY 1,2),


base_lag AS(
  SELECT *,
    LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1

  FROM base_data_pedido),


base_intervalo AS (
  SELECT idVendedor,
    AVG(DATEDIFF(dtPedido, lag1)) AS media_intervalo_vendas
  FROM base_lag

  GROUP BY idVendedor)


SELECT a.*,
  b.min_valor_pedido,
  b.max_valor_pedido,
  c.LTV,
  c.qtd_dias_base,
  d.media_intervalo_vendas,
  '{date}' AS data_referencia,
  NOW() AS dtIngestion

FROM base_resumo AS a

LEFT JOIN base_min_max AS b
  ON a.idVendedor = b.idVendedor

LEFT JOIN base_lifetime AS c
  ON a.idVendedor = c.idVendedor

LEFT JOIN base_intervalo AS d
  ON a.idVendedor = d.idVendedor
