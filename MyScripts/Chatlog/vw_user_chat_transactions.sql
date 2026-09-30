CREATE OR REPLACE VIEW "vw_user_chat_transactions" AS 

SELECT
  YEAR(transaction_start_time) transaction_year
, MONTH(transaction_start_time) transaction_month
, CAST(CEIL((DAY(transaction_start_time) / 7E0)) AS INTEGER) transaction_week
, DATE(transaction_start_time) transaction_date
, (CASE WHEN (transaction_duration_second <= approx_percentile(transaction_duration_second, 2E-1) OVER ()) THEN concat('(1) ', concat(concat(concat(CAST(min(transaction_duration_second) OVER () AS VARCHAR), '-'), CAST(approx_percentile(transaction_duration_second, 2E-1) OVER () AS VARCHAR)), ' sec')) WHEN (transaction_duration_second <= approx_percentile(transaction_duration_second, 4E-1) OVER ()) THEN concat('(2) ', concat(concat(concat(CAST(approx_percentile(transaction_duration_second, 2E-1) OVER () AS VARCHAR), '-'), CAST(approx_percentile(transaction_duration_second, 4E-1) OVER () AS VARCHAR)), ' sec')) WHEN (transaction_duration_second <= approx_percentile(transaction_duration_second, 6E-1) OVER ()) THEN concat('(3) ', concat(concat(concat(CAST(approx_percentile(transaction_duration_second, 4E-1) OVER () AS VARCHAR), '-'), CAST(approx_percentile(transaction_duration_second, 6E-1) OVER () AS VARCHAR)), ' sec')) WHEN (transaction_duration_second <= approx_percentile(transaction_duration_second, 8E-1) OVER ()) THEN concat('(4) ', concat(concat(concat(CAST(approx_percentile(transaction_duration_second, 6E-1) OVER () AS VARCHAR), '-'), CAST(approx_percentile(transaction_duration_second, 8E-1) OVER () AS VARCHAR)), ' sec')) ELSE concat('(5) ', concat(concat(concat(CAST(approx_percentile(transaction_duration_second, 8E-1) OVER () AS VARCHAR), '-'), CAST(max(transaction_duration_second) OVER () AS VARCHAR)), ' sec')) END) transaction_duration_range_second
, WEEK(transaction_start_time) transaction_week_of_year
, QUARTER(transaction_start_time) transaction_quarter_of_year
, MONTH(transaction_start_time) transaction_month_of_year
, DOW(transaction_start_time) transaction_day_of_week
, concat(concat(concat(concat(concat(concat('W', CAST(WEEK(transaction_start_time) AS VARCHAR)), ' ('), DATE_FORMAT(DATE_ADD('day', -((DOW(transaction_start_time) - 1)), transaction_start_time), '%d/%m/%Y')), '-'), DATE_FORMAT(DATE_ADD('day', (7 - DOW(transaction_start_time)), transaction_start_time), '%d/%m/%Y')), ')') transaction_week_range
, *
FROM
  "chatlog_bi_silver"."tbl_user_chat_transaction"
