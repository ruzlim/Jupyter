
/*** vw_transaction_date_agg_period_rank ***/

SELECT 
  transaction_id,
  transaction_date,
  transaction_week_range,
  date_format(transaction_date, '%b-%Y') as transaction_report_month,
  emp_id,
  bu_group,
  CASE 
    WHEN transaction_date = <<$transactionenddate>> THEN 0
    WHEN transaction_date BETWEEN <<$transactionstartdate>> AND <<$transactionenddate>> 
      THEN DENSE_RANK() OVER (ORDER BY transaction_date DESC)
    ELSE -1 
  END AS transaction_period_date_rank
FROM chatlog_bi_gold.vw_user_chat_transactions