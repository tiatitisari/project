{{  config(
        materialized = "incremental",
        incremental_strategy='insert_overwrite',
        on_schema_change="sync_all_columns",
        partition_by = {
            "field": "business_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = [
            "business_unit",
            "partner_name",
            "customer_flagged"
        ]
    )
}}


-- description: average balance and transaction performance All Period
--     config:
--       tags:
--         - deploy-on-ci
--       owner: aprilia
    
{% set watermark = var('wm') %}

{% if is_incremental() %}
  {% set start_date = get_maximum_partition_date(
     dataset_id='dv',
     table_id='transact'
  ) %}
{% else %}
  {% set start_date = get_minimum_partition_date(
     dataset_id='dm',
     table_id='customer'
  ) %}
{% endif %}


 WITH daily_transacting_customers AS (
 SELECT
      bal.business_date,
      customer_source,
      IFNULL(bal.business_unit, '- LFS') AS business_unit,
      IFNULL(bal.customer_flagged, 'LFS Only') AS customer_flagged,
      CASE
        WHEN partner_name = 'Partnership' THEN 'Partners'
        WHEN partner_name LIKE 'Gojek%' OR partner_name LIKE 'GoTo' THEN 'Gojek'
        WHEN partner_name LIKE 'MFS%' OR partner_name LIKE 'WFB%' THEN 'Stand Alone'
        WHEN (partner_name = 'Sharia' AND customer_flagged = 'Sharia Only')
          OR (customer_source = 'LFS' AND customer_flagged IS NULL AND business_unit = '- Syariah' AND partner_name IS NOT NULL)
          THEN 'Sharia Stand Alone'
        WHEN partner_name = 'Sharia' AND customer_flagged = 'Sharia and Amaan' THEN 'Sharia Amaan'
        ELSE partner_name
 END AS partner_name,
      c.customer_id,
      identity_address_province,
      age_group,
      balance_tier_description,
      CASE WHEN c.customer_id IS NOT NULL THEN bal.total_balance ELSE 0 END AS bal_trx,
      bal.customer_id AS customer_id_registered,
      bal.total_balance AS total_balance,
      customer_type,
      SUM(c.transaction_amount) AS transaction_amount,
      SUM(c.transaction_id) AS transaction_id,
    FROM (
      SELECT
        transaction_date,
        customer_id,
        SUM(transaction_amount) AS transaction_amount,
        COUNT(DISTINCT transaction_id) AS transaction_id,
      FROM
        {{ r("transaction_record", has_stub=false) }}
      WHERE
        transaction_date IS NOT NULL
      GROUP BY 1,2
    ) c
    RIGHT JOIN (
      SELECT
        business_date,
        customer_id,
        customer_source,
        customer_type,
        balance_tier_description,
        identity_address_province,
        age_group,
        business_unit,
        customer_flagged,
        partner_name,
        customer_total_balance AS total_balance
      FROM
        {{ r("customer") }}
      WHERE
        business_date >= '2021-01-01'
        {% if is_incremental() %}
          AND LAST_DAY(business_date) >= (SELECT DATE_SUB(MAX(business_date),INTERVAL 1 MONTH )FROM {{ this }})
        {% endif %}
        AND (customer_status IN ('ACTIVE', 'INACTIVE') OR total_balance > 0)
        AND id_number IS NOT NULL
        AND balance_tier_description IS NOT NULL
        AND customer_total_balance IS NOT NULL
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ) bal
    ON
      c.transaction_date = bal.business_date
      AND c.customer_id = bal.customer_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)

 SELECT
 business_date,
  mtc.business_unit,
  customer_flagged,
  age_group,
  identity_address_province,
  last_customer_cluster_by_scoring,
  customer_source,
  IFNULL(partner_name, 'Stand Alone') AS partner_name,
        {{ dashboard_views_balance_tier_mapping("transaction_amount") }}
        END AS transaction_tier_amount,
        {{ dashboard_views_transaction_tier_mapping("transaction_id") }}
        END AS transaction_tier_frequency,
  balance_tier_description,
  customer_type,
  ROW_NUMBER() OVER(ORDER BY business_date DESC) AS rn,
  COUNT(DISTINCT customer_id_registered) AS total_registered,
  COUNT(DISTINCT mtc.customer_id) AS no_of_unique_transacting_customers,
  COUNT(mtc.customer_id) AS unique_transacting_customer,
  SUM(total_balance) AS total_balance,
  SUM(bal_trx) AS balance_transacting_customer,
  IFNULL(SUM(transaction_amount), 0) AS transaction_amount,
  IFNULL(SUM(transaction_id),0) AS transaction_id,
  CURRENT_TIMESTAMP() AS record_inserted_at
FROM
  daily_transacting_customers mtc
  LEFT JOIN
   {{ r("master_db_profile_segment", has_stub=false) }}
   USING(customer_id)
GROUP BY 1,2,3,4,5,6,7,8,11,12
