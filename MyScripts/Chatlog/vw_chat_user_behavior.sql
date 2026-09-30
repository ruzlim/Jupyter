
CREATE OR REPLACE VIEW "vw_chat_user_behavior" AS 

WITH

  user_attributes AS (
   SELECT DISTINCT
     emp_id
   , bu_group
   , emp_position
   , internal_team_flag
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
) 

, weekly_stats AS (
   SELECT
     emp_id
   , YEAR(transaction_start_time) transaction_year
   , MONTH(transaction_start_time) transaction_month
   , CAST(CEIL((DAY(transaction_start_time) / 7E0)) AS INTEGER) transaction_week
   , COUNT(DISTINCT transaction_id) weekly_transactions
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE (transaction_start_time IS NOT NULL)
   GROUP BY emp_id, YEAR(transaction_start_time), MONTH(transaction_start_time), CEIL((DAY(transaction_start_time) / 7E0))
) 

, last_transaction AS (
   SELECT
     emp_id
   , DATE_DIFF('day', MAX(DATE(transaction_start_time)), current_date) days_since_last
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE (transaction_start_time IS NOT NULL)
   GROUP BY emp_id
) 

, overall_stats AS (
   SELECT
     emp_id
   , COUNT(DISTINCT transaction_id) total_transactions
   , CEIL(AVG(transaction_duration_second)) avg_duration_seconds
   , LEAST(ROUND(((SUM((CASE WHEN (transaction_success_flag = 'Y') THEN 1 ELSE 0 END)) * 1E2) / COUNT(*)), 2), 1E2) success_rate_percent
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE (transaction_start_time IS NOT NULL)
   GROUP BY emp_id
) 

, weeks_active AS (
   SELECT
     emp_id
   , COUNT(DISTINCT concat(concat(CAST(YEAR(transaction_start_time) AS VARCHAR), '-'), CAST(WEEK(transaction_start_time) AS VARCHAR))) total_weeks_active
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE (transaction_start_time IS NOT NULL)
   GROUP BY emp_id
   HAVING (COUNT(DISTINCT transaction_id) > 1)
) 

, last_active_week AS (
   SELECT
     emp_id
   , MAX(WEEK(transaction_start_time)) last_week_with_transaction
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE ((transaction_start_time IS NOT NULL) AND (YEAR(transaction_start_time) = YEAR(current_date)))
   GROUP BY emp_id
) 

, user_behavior AS (
   SELECT
     t.emp_id
   , (CASE WHEN ((COUNT(DISTINCT (CASE WHEN (WEEK(t.transaction_start_time) = COALESCE(lw.last_week_with_transaction, WEEK(current_date))) THEN t.transaction_id END)) > 0) AND (COUNT(DISTINCT (CASE WHEN (WEEK(t.transaction_start_time) = COALESCE(lw.last_week_with_transaction, WEEK(current_date))) THEN t.transaction_id END)) = COUNT(DISTINCT t.transaction_id))) THEN true ELSE false END) is_first_time_user
   , (CASE WHEN (COUNT(DISTINCT (CASE WHEN (WEEK(t.transaction_start_time) = COALESCE(lw.last_week_with_transaction, WEEK(current_date))) THEN t.transaction_id END)) > 0) THEN true ELSE false END) current_week_usage
   , (CASE WHEN (COUNT(DISTINCT (CASE WHEN (WEEK(t.transaction_start_time) = (COALESCE(lw.last_week_with_transaction, WEEK(current_date)) - 1)) THEN t.transaction_id END)) > 0) THEN true ELSE false END) week_minus1_usage
   , (CASE WHEN (COUNT(DISTINCT (CASE WHEN (WEEK(t.transaction_start_time) = (COALESCE(lw.last_week_with_transaction, WEEK(current_date)) - 2)) THEN t.transaction_id END)) > 0) THEN true ELSE false END) week_minus2_usage
   FROM
     (chatlog_bi_silver.tbl_user_chat_transaction t
   LEFT JOIN last_active_week lw ON (t.emp_id = lw.emp_id))
   WHERE ((t.transaction_start_time IS NOT NULL) AND (YEAR(t.transaction_start_time) = YEAR(current_date)))
   GROUP BY t.emp_id, lw.last_week_with_transaction
) 

, topic_stats AS (
   SELECT
     emp_id
   , transaction_response_topic
   , COUNT(DISTINCT transaction_id) topic_count
   , ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY COUNT(DISTINCT transaction_id) DESC) rn
   FROM
     chatlog_bi_silver.tbl_user_chat_transaction
   WHERE ((transaction_start_time IS NOT NULL) AND (transaction_response_topic IS NOT NULL))
   GROUP BY emp_id, transaction_response_topic
) 

, most_common_topic AS (
   SELECT
     emp_id
   , transaction_response_topic most_common_topic
   FROM
     topic_stats
   WHERE (rn = 1)
) 

SELECT
  COALESCE(w.emp_id, l.emp_id, o.emp_id, b.emp_id, wa.emp_id, mt.emp_id) emp_id
, ua.bu_group
, ua.emp_position
, ua.internal_team_flag
, w.transaction_year
, w.transaction_month
, w.transaction_week
, w.weekly_transactions
, COALESCE(l.days_since_last, 0) days_since_last_transaction
, o.total_transactions
, o.avg_duration_seconds
, o.success_rate_percent
, COALESCE(wa.total_weeks_active, 0) total_weeks_active
, b.is_first_time_user
, b.current_week_usage
, b.week_minus1_usage
, b.week_minus2_usage
, (CASE WHEN ((b.current_week_usage = true) AND (b.week_minus1_usage = true) AND (b.week_minus2_usage = true) AND (b.is_first_time_user = false)) THEN 'active' WHEN ((b.current_week_usage = true) AND (b.week_minus1_usage = false) AND (b.week_minus2_usage = false) AND (b.is_first_time_user = true)) THEN 'new' WHEN ((b.current_week_usage = true) AND (b.week_minus1_usage = false) AND (b.week_minus2_usage = false) AND (b.is_first_time_user = false)) THEN 'retention' ELSE 'inactive' END) user_segment
, mt.most_common_topic
, current_date report_date

FROM
  ((((((weekly_stats w
FULL JOIN last_transaction l ON (w.emp_id = l.emp_id))
FULL JOIN overall_stats o ON (COALESCE(w.emp_id, l.emp_id) = o.emp_id))
FULL JOIN weeks_active wa ON (COALESCE(w.emp_id, l.emp_id, o.emp_id) = wa.emp_id))
FULL JOIN user_behavior b ON (COALESCE(w.emp_id, l.emp_id, o.emp_id, wa.emp_id) = b.emp_id))
FULL JOIN most_common_topic mt ON (COALESCE(w.emp_id, l.emp_id, o.emp_id, wa.emp_id, b.emp_id) = mt.emp_id))
LEFT JOIN user_attributes ua ON (COALESCE(w.emp_id, l.emp_id, o.emp_id, wa.emp_id, b.emp_id, mt.emp_id) = ua.emp_id))

ORDER BY emp_id ASC, w.transaction_year DESC, w.transaction_month DESC, w.transaction_week DESC
