
/*** chat_transaction_data ***/

SELECT
    t.transaction_year,
    t.transaction_month,
    t.transaction_week,
    t.transaction_date,
    t.transaction_duration_range_second,
    t.transaction_week_of_year,
    t.transaction_quarter_of_year,
    t.transaction_month_of_year,
    t.transaction_day_of_week,
    t.transaction_week_range,
    t.transaction_id,
    t.chat_log_id,
    t.language,
    t.transaction_prompt,
    t.transaction_prompt_topic,
    t.transaction_response,
    t.transaction_response_topic,
    t.transaction_success_flag,
    t.error_message,
    t.emp_id,
    t.internal_team_flag,
    t.transaction_start_time,
    t.transaction_end_time,
    t.transaction_duration_second,
    t.bu_group,
    t.emp_position,
    t.area_type,
    t.area_code,
    t.area_name,
    t.region_code,
    t.region_name,
    t.transaction_prompt_timestamp,
    t.load_timestamp,
    t.data_dt,
    u.user_id,
    u.user_transaction_year,
    u.usr_transaction_month,
    u.user_transaction_week,
    u.user_segment,
    u.user_report_date
FROM chatlog_bi_gold.vw_user_chat_transactions t
LEFT JOIN (
    SELECT
        emp_id          AS user_id,
        transaction_year  AS user_transaction_year,
        transaction_month AS usr_transaction_month,
        transaction_week  AS user_transaction_week,
        user_segment,
        report_date       AS user_report_date
    FROM chatlog_bi_gold.vw_chat_user_behavior
) u
ON  t.emp_id            = u.user_id
AND t.transaction_year  = u.user_transaction_year
AND t.transaction_month = u.usr_transaction_month
AND t.transaction_week  = u.user_transaction_week