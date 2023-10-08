/* Author: Aprilia Evans
   Purpose: to generate view for balance injection*/

{% macro campaign_balance_injection(
    campaign_name,
    campaign_start_date,
    campaign_end_date,
    note_1,
    maximum_cap,
    incentive_note,
    incentive_code,
    bankcode,
    amount,
    deep_link,
    web_link) %}

WITH vars AS (
  SELECT
     DATE('{{ campaign_start_date }}')AS dstart
    , DATE('{{ campaign_end_date }}') AS dend
)

, eligible_customers AS (
  SELECT
    account_number
    , bank_code
    , cif
    , customer_id
    , note_1
    , note_2
    , note_5
    , preferred_language
    , amount
  FROM
    {{ s('offers', 'offers_population', has_stub=false) }}  t
  WHERE
    DATE(created_at, "Asia/Jakarta") >= (SELECT dstart FROM vars)
    AND campaign_name = '{{ campaign_name }}'
    AND lower(note_1) LIKE '%{{ note_1 }}%'
)

, disbursed_customers AS (
  SELECT customer_id
  FROM
    {{ r('offers__customer_disbursed_incentive', has_stub=false) }}
  WHERE
    incentive_disbursed_date BETWEEN (SELECT dstart FROM vars) AND (SELECT DATE_ADD(dend, INTERVAL 1 DAY) FROM vars)
    AND incentive_code = '{{ incentive_code }}'
    AND incentive_note like '{{ incentive_note }}'
)

, final_cte AS (
  SELECT
    eligible_customers.customer_id
    , account_number
    , cif
    , note_2
    , preferred_language
    , amount
    , ROW_NUMBER() OVER (ORDER BY eligible_customers.customer_id, note_1) AS rn
  FROM
    eligible_customers
  LEFT JOIN
    disbursed_customers AS dcus
    ON eligible_customers.customer_id = dcus.customer_id
  WHERE
    dcus.customer_id IS NULL
)

SELECT
  CAST(account_number AS STRING) AS accountNumber
  , CAST('{{ bankcode }}' AS STRING) AS bankCode
  , CAST(cif AS STRING) AS cif
	, CAST(customer_id AS STRING) AS customerId
  {% if amount >0 %}
	, CAST({{ amount }} AS NUMERIC) AS amount
  {% else %}
  , CAST(amount AS NUMERIC) AS amount
  {% endif %}
  , TO_BASE64(SHA256(CONCAT(customer_id,'{{ campaign_name }}','{{ campaign_start_date}}'))) AS idempotencyKey
	, STRUCT (
        STRUCT (
            '{{ deep_link }}' AS deepLink
            , '{{ web_link }}' AS webLink
        ) AS extra
    ) AS notificationPayload
FROM
  final_cte
WHERE rn <= {{ maximum_cap }}
GROUP BY 1,2,3,4,5,6

{% endmacro %}
