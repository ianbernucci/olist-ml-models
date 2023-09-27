-- Databricks notebook source
DROP TABLE IF EXISTS silver.analytics.abt_olist_churn;
CREATE TABLE silver.analytics.abt_olist_churn

WITH tb_features AS (
    SELECT 
          t1.data_referencia,
          t1.idVendedor,
          t1.qtd_pedidos,
          t1.qtd_dias,
          t1.qtd_itens,
          t1.qtd_recencia,
          t1.media_ticket,
          t1.media_valor_produto,
          t1.max_valor_produto,
          t1.min_valor_produto,
          t1.media_produto_pedido,
          t1.min_valor_pedido,
          t1.max_valor_pedido,
          t1.LTV,
          t1.qtd_dias_base,
          t1.media_intervalo_vendas,

          t2.media_nota,
          t2.mediana_nota,
          t2.min_nota,
          t2.max_nota,
          t2.perc_avaliacao,

          t3.qnt_UFs_atendidos,
          t3.perc_pedidos_AC,
          t3.perc_pedidos_AL,
          t3.perc_pedidos_AM,
          t3.perc_pedidos_AP,
          t3.perc_pedidos_BA,
          t3.perc_pedidos_CE,
          t3.perc_pedidos_DF,
          t3.perc_pedidos_ES,
          t3.perc_pedidos_GO,
          t3.perc_pedidos_MA,
          t3.perc_pedidos_MG,
          t3.perc_pedidos_MS,
          t3.perc_pedidos_MT,
          t3.perc_pedidos_PA,
          t3.perc_pedidos_PB,
          t3.perc_pedidos_PE,
          t3.perc_pedidos_PI,
          t3.perc_pedidos_PR,
          t3.perc_pedidos_RJ,
          t3.perc_pedidos_RN,
          t3.perc_pedidos_RO,
          t3.perc_pedidos_RR,
          t3.perc_pedidos_RS,
          t3.perc_pedidos_SC,
          t3.perc_pedidos_SE,
          t3.perc_pedidos_SP,
          t3.perc_pedidos_TO,

          t5.qtd_vendas_boleto,
          t5.qtd_vendas_credit_card,
          t5.qtd_vendas_voucher,
          t5.qtd_vendas_debit_card,
          t5.valor_vendas_boleto,
          t5.valor_vendas_credit_card,
          t5.valor_vendas_voucher,
          t5.valor_vendas_debit_card,
          t5.perc_qtd_vendas_boleto,
          t5.perc_qtd_vendas_credit_card,
          t5.perc_qtd_vendas_voucher,
          t5.perc_qtd_vendas_debit_card,
          t5.perc_valor_vendas_boleto,
          t5.perc_valor_vendas_credit_card,
          t5.perc_valor_vendas_voucher,
          t5.perc_valor_vendas_debit_card,
          t5.media_de_parcelas,
          t5.mediana_de_parcelas,
          t5.max_parcelas,
          t5.min_parcelas,

          t6.media_fotos,
          t6.media_volume,
          t6.mediana_volume,
          t6.min_volume,
          t6.max_volume,
          t6.perc_categoria_cama_mesa_banho,
          t6.perc_categoria_beleza_saude,
          t6.perc_categoria_esporte_lazer,
          t6.perc_categoria_informatica_acessorios,
          t6.perc_categoria_moveis_decoracao,
          t6.perc_categoria_utilidades_domesticas,
          t6.perc_categoria_relogios_presentes,
          t6.perc_categoria_telefonia,
          t6.perc_categoria_automotivo,
          t6.perc_categoria_brinquedos,
          t6.perc_categoria_cool_stuff,
          t6.perc_categoria_ferramentas_jardim,
          t6.perc_categoria_perfumaria,
          t6.perc_categoria_bebes,
          t6.perc_categoria_eletronicos

    FROM silver.analytics.fs_vendedor_vendas AS t1

    LEFT JOIN silver.analytics.fs_vendedor_avaliacao AS t2
    ON t1.idVendedor = t2.idVendedor
    AND t1.data_referencia = t2.data_referencia

    LEFT JOIN silver.analytics.fs_vendedor_cliente AS t3
    ON t1.idVendedor = t3.idVendedor
    AND t1.data_referencia = t3.data_referencia

    LEFT JOIN silver.analytics.fs_vendedor_entrega AS t4
    ON t1.idVendedor = t4.idVendedor
    AND t1.data_referencia = t4.data_referencia

    LEFT JOIN silver.analytics.fs_vendedor_pagamentos AS t5
    ON t1.idVendedor = t5.idVendedor
    AND t1.data_referencia = t5.data_referencia

    LEFT JOIN silver.analytics.fs_vendedor_produto AS t6
    ON t1.idVendedor = t6.idVendedor
    AND t1.data_referencia = t6.data_referencia

    WHERE t1.qtd_recencia <= 45

),

tb_event AS (
  SELECT distinct idVendedor,
         DATE(dtPedido) as dtPedido

  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE idVendedor is not null
),

tb_flag AS (

  SELECT t1.data_referencia,
        t1.idVendedor,
        min(t2.dtPedido) as dtProxPedido

  FROM tb_features AS t1

  LEFT JOIN tb_event AS t2
  ON t1.idVendedor = t2.idVendedor
  AND t1.data_referencia <= t2.dtPedido
  AND datediff(dtPedido, data_referencia) <= 45 - qtd_recencia

  GROUP BY 1,2

)

SELECT t1.*,
       CASE WHEN dtProxPedido IS NULL THEN 1 ELSE 0 END AS flChurn

FROM tb_features AS t1

LEFT JOIN tb_flag AS t2
ON t1.idVendedor = t2.idVendedor
AND t1.data_referencia = t2.data_referencia

WHERE DAY(t1.data_referencia) = 1

ORDER BY t1.idVendedor, t1.data_referencia;
