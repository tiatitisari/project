WITH dummy_cluster AS (
  SELECT DISTINCT customer_id, 
  FROM `database.database` 
  WHERE customer_start_date BETWEEN '2022-01-01' AND '2022-01-09'
  AND LOWER(customer_status) = 'active'
  ),-- check null passed 
    -- check duplicate passed 
    account AS (SELECT DISTINCT a.customer_id,
    b.account_number, savings_account_type
    FROM dummy_cluster a
    JOIN 
    `database.savings` b 
    USING (customer_id)
    ), 
    -------Total number of  pockets, each pocket
    total_pot AS (
      SELECT 
      customer_id,savings_account_type,
      COUNT(savings_account_type) as total_each_pocket, 
      COUNT(account_number) AS total_pocket, 
      FROM account 
      GROUP BY 1,2
      ORDER BY (total_pocket) ASC
    ),

    -----Total number of monthly active  pockets
    transaction_id AS(
      SELECT a.account_number, 
      CASE WHEN EXTRACT(MONTH FROM transaction_event_at) =1 THEN 'jan' 
          WHEN EXTRACT(MONTH FROM transaction_event_at) =2 THEN 'feb'
          WHEN EXTRACT(MONTH FROM transaction_event_at) =3 THEN 'mar' 
          WHEN EXTRACT(MONTH FROM transaction_event_at) =4 THEN 'april' 
          END AS month,
      COUNT(DISTINCT b.transaction_id) as total_transaction
      FROM account a LEFT JOIN 
      (SELECT source_account_number, 
      transaction_id, 
      transaction_event_at FROM `database.core_transaction` 
      WHERE transaction_category NOT IN ('INF','INT','TAX','REV','BI','GL','FEE')
      AND DATE(transaction_event_at)
      BETWEEN '2022-01-01' AND '2022-04-30') b ON 
      a.account_number = b.source_account_number 
      GROUP BY 1,2
    ),
    total_acc AS(
      SELECT account_number, month, 
      CASE WHEN total_transaction>0 THEN 1 ELSE 0 END AS total_account
      FROM transaction_id 
    ),
    total_month AS(
      SELECT month,SUM(total_account) as total_account 
      FROM total_acc
      GROUP BY 1
    ), 
    avg_total AS(
      SELECT AVG(total_account) as avg_total_4_month
      FROM total_month 
    ),
    ----Percentile distribution of pockets per user
    percentile AS (
      SELECT
      PERCENTILE_DISC(total_pocket,0.01) OVER () AS percentile1,
      PERCENTILE_DISC(total_pocket,0.5) OVER () AS percentile_50,
      PERCENTILE_DISC(total_pocket,0.75) OVER () AS percentile_75,
      PERCENTILE_DISC(total_pocket,0.9) OVER () AS percentile_90, 
      FROM total_pot
    ),---Total number of debit cards, physical + virtual card
    account_card AS (
      SELECT 
      a.customer_id,
      b.cif, 
      b.card_type, 
      b.card_id,
      ARRAY_TO_STRING(b.linked_account_numbers, '[]') as account_number, 
      DATE(b.expiration_timestamp) AS expired,
      c.card_form 
      FROM dummy_cluster a
      JOIN 
      `-bank-data-production.dwh_core.card_current_snapshot` b 
      USING (customer_id)
      JOIN 
      (SELECT DISTINCT 
      card_type, 
      card_form 
      FROM`database.card_product`) c
      ON b.card_type = c.card_type 
    ),--Total number of monthly active debit cards
    transaction_card AS(
      SELECT a.account_number, 
      CASE WHEN EXTRACT(MONTH FROM transaction_event_at) =1 THEN 'jan' 
          WHEN EXTRACT(MONTH FROM transaction_event_at) =2 THEN 'feb'
          WHEN EXTRACT(MONTH FROM transaction_event_at) =3 THEN 'mar' 
          WHEN EXTRACT(MONTH FROM transaction_event_at) =4 THEN 'april' 
          END AS month,
      COUNT(DISTINCT b.transaction_id) as total_transaction
      FROM account_card a LEFT JOIN 
      (SELECT source_account_number, 
      transaction_id, 
      transaction_event_at FROM `database.core_transaction` 
      WHERE transaction_category NOT IN ('INF','INT','TAX','REV','BI','GL','FEE')
      AND DATE(transaction_event_at)
      BETWEEN '2022-01-01' AND '2022-04-30') b ON 
      a.account_number = b.source_account_number 
      GROUP BY 1,2
    ),
    total_acc_card AS(
      SELECT account_number, month, 
      CASE WHEN total_transaction>0 THEN 1 ELSE 0 END AS total_account
      FROM transaction_card 
    ),
    total_month_card AS(
      SELECT month,SUM(total_account) as total_account 
      FROM total_acc_card
      GROUP BY 1
    ), 
    avg_total_card AS(
      SELECT AVG(total_account) as avg_total_4_month
      FROM total_month_card
    )

SELECT * FROM avg_total_card