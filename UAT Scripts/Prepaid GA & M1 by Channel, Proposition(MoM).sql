

/*** Prepaid GA & M1 by Channel, Proposition(MoM) ***/

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


WITH W_PARAM AS
(
    SELECT p_start_date, p_end_date
		, SUBSTRING(p_end_date, 7, 2)::INT AS mom_day
		, SUBSTRING(p_end_date, 1, 6)::INT AS curr_tm_key_mth
	FROM ( 
		SELECT 
			-- 20260802::INTEGER AS p_start_date, 20260802::INTEGER AS p_end_date 
			20260501::INTEGER AS p_start_date, 20260615::INTEGER AS p_end_date 
	) TMP
)

-- SELECT * FROM W_PARAM
-----------------------------------------------------------------------------------------------------------------------


, W_ORG AS 
(
	SELECT DISTINCT zone_type
		, orgid_g, tds_sgmd
		, orgid_h, hop_hint
		, SUBSTRING(ccaatt,1,2) AS province_cd, province_eng
		, orgid_hh, d_cluster
		, SUBSTRING(ccaatt,1,4) AS ccaa, district_en, district_th 
		, ccaatt, sub_district_en, sub_district_th 
	FROM EDMAIML_CENTRAL_DATA.DIM_MOOC_AREA
	WHERE team_code <> 'ไม่ระบุ' AND remark <> 'Dummy'
	AND hop_hint = 'CHIANG MAI 1'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, group_channel, tds_special_channel, proposition
		, ga, m1
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, group_channel, tds_special_channel, gp_special_sku AS proposition
			, SUM(activation) AS ga
			, SUM(activation_value) AS m1
		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.P_START_DATE AND P.P_END_DATE
		AND sub_product IN ('PREPAY', 'INFLOW_M1')
		AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, group_channel, tds_special_channel, gp_special_sku
	) TMP
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-- ORDER BY tm_key_day, product, group_channel, tds_special_channel, ga DESC, proposition
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_CHANNEL_MOM AS 
(
	SELECT tm_key_mth, product, group_channel, tds_special_channel
		, ga_mtd_channel, prev_ga_mtd_channel, ga_mom_channel
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_channel DESC NULLS LAST, group_channel, tds_special_channel) AS ga_top_channel_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_channel NULLS LAST, group_channel, tds_special_channel) AS ga_bot_channel_rnk
		, m1_mtd_channel, prev_m1_mtd_channel, m1_mom_channel
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_channel DESC NULLS LAST, m1_mom_channel DESC NULLS LAST, group_channel, tds_special_channel) AS m1_top_channel_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_channel NULLS LAST, m1_mom_channel NULLS LAST, group_channel, tds_special_channel) AS m1_bot_channel_rnk
	FROM (
		SELECT tm_key_mth, product, group_channel, tds_special_channel
			, ga_mtd_channel, prev_ga_mtd_channel
			, CASE 	WHEN prev_ga_mtd_channel <> 0 
					THEN (ga_mtd_channel - prev_ga_mtd_channel) / prev_ga_mtd_channel * 100 
					END ga_mom_channel
			, m1_mtd_channel, prev_m1_mtd_channel
			, CASE 	WHEN prev_m1_mtd_channel <> 0 
					THEN (m1_mtd_channel - prev_m1_mtd_channel) / prev_m1_mtd_channel * 100 
					END m1_mom_channel
		FROM (
			SELECT tm_key_mth, product, group_channel, tds_special_channel
				, ga_mtd_channel
				, LAG(ga_mtd_channel) OVER (PARTITION BY group_channel, tds_special_channel ORDER BY tm_key_mth) AS prev_ga_mtd_channel
				, m1_mtd_channel
				, LAG(m1_mtd_channel) OVER (PARTITION BY group_channel, tds_special_channel ORDER BY tm_key_mth) AS prev_m1_mtd_channel
			FROM (
				SELECT tm_key_mth, product, group_channel, tds_special_channel
					, SUM(ga) AS ga_mtd_channel
					, SUM(m1) AS m1_mtd_channel
				FROM W_PREPAID
				WHERE mom_flag = 'Y'
				GROUP BY tm_key_mth, product, group_channel, tds_special_channel
			) T1
		) T2
	) T3
	WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
	AND ga_mtd_channel <> 0
	-- AND m1_mtd_channel <> 0
) --> W_TXN_CHANNEL_MOM

-- SELECT * FROM W_TXN_CHANNEL_MOM
-- -- WHERE (ga_top_channel_rnk <= 3 OR ga_bot_channel_rnk <= 3)
-- ORDER BY ga_top_channel_rnk
-- -- ORDER BY ga_bot_channel_rnk
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
		, ga_mtd, prev_ga_mtd
		, CASE 	WHEN prev_ga_mtd <> 0 
				THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 
				END ga_mom
		, m1_mtd, prev_m1_mtd
		, CASE 	WHEN prev_m1_mtd <> 0 
				THEN (m1_mtd - prev_m1_mtd) / prev_m1_mtd * 100 
				END m1_mom
	FROM (
		SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY group_channel, tds_special_channel, proposition ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY group_channel, tds_special_channel, proposition ORDER BY tm_key_mth) AS prev_m1_mtd
		FROM (
			SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, group_channel, tds_special_channel, proposition
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-- ORDER BY tm_key_mth, group_channel, tds_special_channel, ga_mtd DESC, proposition
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.group_channel, A.tds_special_channel, A.proposition
		, A.ga_mtd, B.ga_mom, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, B.m1_mom, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
	FROM (
		SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
			, SUM(ga) AS ga_mtd
			, SUM(m1) AS m1_mtd
		FROM W_PREPAID
		WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
		GROUP BY tm_key_mth, product, group_channel, tds_special_channel, proposition
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.group_channel = A.group_channel
		AND B.tds_special_channel = A.tds_special_channel
		AND B.proposition = A.proposition
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-- ORDER BY tm_key_mth, group_channel, tds_special_channel, ga_mtd DESC, proposition
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
	, ga_mtd, ga_mom--, ga_mtd_cal, prev_ga_mtd
	, ga_mtd_channel--, ga_top_channel_rnk, ga_bot_channel_rnk
	, ga_top_rnk--, ga_bot_rnk
	-- , m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd, m1_mtd_channel, m1_top_channel_rnk, m1_bot_channel_rnk, m1_top_rnk, m1_bot_rnk

FROM (
	SELECT tm_key_mth, product, group_channel, tds_special_channel, proposition
		, ga_mtd, ga_mom, ga_mtd_cal, prev_ga_mtd, ga_mtd_channel, ga_top_channel_rnk, ga_bot_channel_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth, product, group_channel, tds_special_channel ORDER BY ga_mtd DESC NULLS LAST, proposition) AS ga_top_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth, product, group_channel, tds_special_channel ORDER BY ga_mtd NULLS LAST, proposition) AS ga_bot_rnk
		-- , m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd, m1_mtd_channel, m1_top_channel_rnk, m1_bot_channel_rnk
		-- , DENSE_RANK() OVER (PARTITION BY tm_key_mth, product, group_channel, tds_special_channel ORDER BY m1_mtd DESC NULLS LAST, proposition) AS m1_top_rnk
		-- , DENSE_RANK() OVER (PARTITION BY tm_key_mth, product, group_channel, tds_special_channel ORDER BY m1_mtd NULLS LAST, proposition) AS m1_bot_rnk
	FROM (
		SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS group_channel, 'ALL' AS tds_special_channel, 'ALL' AS proposition
			-- Gross Adds
			, ga_mtd
			, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
			, ga_mtd_cal, prev_ga_mtd
			, NULL AS ga_mtd_channel, NULL AS ga_top_channel_rnk, NULL AS ga_bot_channel_rnk
			-- Inflow M1
			, m1_mtd
			, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
			, m1_mtd_cal, prev_m1_mtd
			, NULL AS m1_mtd_channel, NULL AS m1_top_channel_rnk, NULL AS m1_bot_channel_rnk
		FROM (
			SELECT tm_key_mth
				, SUM(ga_mtd) AS ga_mtd
				, SUM(ga_mtd_cal) AS ga_mtd_cal
				, SUM(prev_ga_mtd) AS prev_ga_mtd
				, SUM(m1_mtd) AS m1_mtd
				, SUM(m1_mtd_cal) AS m1_mtd_cal
				, SUM(prev_m1_mtd) AS prev_m1_mtd
			FROM W_TXN_MTD
			GROUP BY tm_key_mth
		) TOTAL_MTD
		
		UNION ALL 
		
		SELECT A.tm_key_mth, A.product, A.group_channel, A.tds_special_channel, A.proposition
			, ga_mtd, ga_mom, ga_mtd_cal, prev_ga_mtd, C.ga_mtd_channel, C.ga_top_channel_rnk, C.ga_bot_channel_rnk
			, m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd, C.m1_mtd_channel, C.m1_top_channel_rnk, C.m1_bot_channel_rnk
		FROM W_TXN_MTD A
		INNER JOIN W_TXN_CHANNEL_MOM C
			ON C.tm_key_mth = A.tm_key_mth
			AND C.group_channel = A.group_channel
			AND C.tds_special_channel = A.tds_special_channel
			-- AND (C.ga_top_channel_rnk <= 3 OR C.ga_bot_channel_rnk <= 3)
			-- AND (C.m1_top_channel_rnk <= 3 OR C.m1_bot_channel_rnk <= 3)
	) T
) MTD_SUMMARY

WHERE ga_top_rnk <= 5 AND ga_mtd <> 0

ORDER BY tm_key_mth, product, ga_top_channel_rnk, ga_top_rnk, proposition