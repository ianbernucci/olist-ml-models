WITH base_geral AS (
SELECT a.*,
  c.idVendedor

FROM silver.olist.pagamento_pedido AS a

LEFT JOIN silver.olist.pedido AS b
  ON a.idPedido = b.idPedido  

LEFT JOIN silver.olist.item_pedido AS c
  ON a.idPedido = c.idPedido

WHERE c.idVendedor IS NOT NULL
  AND b.dtPedido < '{date}'
  AND b.dtPedido >= ADD_MONTHS('{date}', -6)),

base_agrupada AS (
SELECT idVendedor,
  descTipoPagamento,
  COUNT(DISTINCT idPedido) AS qtd_vendas,
  SUM(vlPagamento) AS valor_total

FROM base_geral

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor),

base_perc_pagamentos AS (
SELECT idVendedor,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN valor_total ELSE 0 END) AS valor_vendas_credit_card,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_credit_card,

  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_debit_card,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN valor_total ELSE 0 END) AS valor_vendas_debit_card,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_debit_card,

  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_boleto,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN valor_total ELSE 0 END) AS valor_vendas_boleto,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_boleto,

  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_vendas ELSE 0 END) AS qtd_vendas_voucher,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN valor_total ELSE 0 END) AS valor_vendas_voucher,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtd_vendas ELSE 0 END) / SUM(qtd_vendas) AS perc_qtd_vendas_voucher

FROM base_agrupada

GROUP BY idVendedor),

base_cartao AS (
SELECT idVendedor,
AVG(nrParcelas) AS media_de_parcelas,
PERCENTILE(nrParcelas, 0.5) AS mediana_de_parcelas,
MAX(nrParcelas) AS max_parcelas,
MIN(nrParcelas) AS min_parcelas

FROM base_geral

WHERE descTipoPagamento = 'credit_card'

GROUP BY idVendedor)

SELECT a.*,
  b.media_de_parcelas,
  b.mediana_de_parcelas,
  b.max_parcelas,
  b.min_parcelas,
  '{date}'  AS data_refenrencia,
  NOW() AS dtIngestion

FROM base_perc_pagamentos AS a

LEFT JOIN base_cartao AS b
  ON a.idVendedor = b.idVendedor  
