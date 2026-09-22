/*** 
	Prepaid GA & M1 by Partner, SKU(MoM) 

		Test Case: 11419, 11396, 11453, 11442, 11387, 11412, 11377, 11375, 11400, 11474
***/

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
			-- 20260701::INTEGER AS p_start_date, 20260802::INTEGER AS p_end_date 
			-- 20260601::INTEGER AS p_start_date, 20260731::INTEGER AS p_end_date 
			-- 20260625::INTEGER AS p_start_date, 20260705::INTEGER AS p_end_date 
			20260501::INTEGER AS p_start_date, 20260630::INTEGER AS p_end_date 
			-- 20260501::INTEGER AS p_start_date, 20260610::INTEGER AS p_end_date 
			-- 20260401::INTEGER AS p_start_date, 20260512::INTEGER AS p_end_date 
			-- 20260101::INTEGER AS p_start_date, 20260110::INTEGER AS p_end_date 
			-- 20251201::INTEGER AS p_start_date, 20260131::INTEGER AS p_end_date 
			-- 20250915::INTEGER AS p_start_date, 20250915::INTEGER AS p_end_date 
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
	-- AND tds_sgmd = 'North'
	-- AND hop_hint = 'CHIANG MAI 1'
	AND d_cluster LIKE 'CHIANG MAI%'
	-- AND province_eng = 'Chiang Mai'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


-- , W_PREPAID_PARTNER AS --by partner
-- (
-- 	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
-- 		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
-- 		, product, partner_code, partner_name
-- 		, ga
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga NULLS LAST, partner_name DESC) AS ga_bot_partner_rnk
-- 		, m1
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY m1 DESC NULLS LAST, partner_name) AS m1_top_partner_rnk
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY m1 NULLS LAST, partner_name DESC) AS m1_bot_partner_rnk
-- 	FROM (
-- 		SELECT tm_key_mth, tm_key_day
-- 			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
-- 			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
-- 			, P.mom_day
-- 			, product, partner_code, partner_name
-- 			, SUM(activation) AS ga
-- 			, SUM(activation_value) AS m1
-- 		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
-- 		CROSS JOIN W_PARAM P 
-- 		WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
-- 		AND sub_product IN ('PREPAY', 'INFLOW_M1')
-- 		AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
-- 		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, partner_code, partner_name
-- 	) T1
-- 	-- WHERE ga <> 0
-- 	WHERE m1 <> 0
-- ) --> W_PREPAID

-- SELECT tm_key_mth, tm_key_day, product, partner_code, partner_name
-- 	-- , ga, ga_top_partner_rnk, ga_bot_partner_rnk
-- 	, m1, m1_top_partner_rnk, m1_bot_partner_rnk
-- FROM W_PREPAID_PARTNER
-- -- WHERE (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3) ORDER BY tm_key_day, ga_top_partner_rnk
-- WHERE (m1_top_partner_rnk <= 3 OR m1_bot_partner_rnk <= 3) 
-- ORDER BY tm_key_day, m1_top_partner_rnk
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS --by partner, group_sim
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month, mom_flag, product, partner_code, partner_name, group_sim
		, ga, ga_partner
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga_partner DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga_partner NULLS LAST, partner_name DESC) AS ga_bot_partner_rnk
		, m1, m1_partner
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY m1_partner DESC NULLS LAST, partner_name) AS m1_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY m1_partner NULLS LAST, partner_name DESC) AS m1_bot_partner_rnk
	FROM (
		SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
			, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
			, product, partner_code, partner_name, group_sim
			, ga, m1
			, SUM(ga) OVER (PARTITION BY tm_key_day, partner_code) AS ga_partner
			, SUM(m1) OVER (PARTITION BY tm_key_day, partner_code) AS m1_partner
		FROM (
			SELECT tm_key_mth, tm_key_day
				, SUBSTRING(tm_key_day, 7, 2)::INT AS day
				, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
				, P.mom_day
				, product, partner_code, partner_name, group_sim
				, SUM(activation) AS ga
				, SUM(activation_value) AS m1
			FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
			CROSS JOIN W_PARAM P 
			WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
			AND sub_product IN ('PREPAY', 'INFLOW_M1')
			-- AND group_channel LIKE '%Branded Retail%' --True Shop ?
			AND tds_special_channel LIKE '%COM7%'
			-- AND tds_special_channel LIKE '%True Shop%'
			-- AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') AND partner_code LIKE '711%'
			AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
			GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, partner_code, partner_name, group_sim
		) T1
	) T2
	-- WHERE ga <> 0
	-- WHERE m1 <> 0
) --> W_PREPAID

-- SELECT tm_key_mth, tm_key_day, product, partner_code, partner_name, group_sim
-- 	-- , ga, ga_partner, ga_top_partner_rnk, ga_bot_partner_rnk
-- 	, m1, m1_partner, m1_top_partner_rnk, m1_bot_partner_rnk
-- FROM W_PREPAID
-- -- WHERE (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3) ORDER BY tm_key_day, ga_partner DESC
-- WHERE (m1_top_partner_rnk <= 3 OR m1_bot_partner_rnk <= 3) ORDER BY tm_key_day, m1_partner DESC
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_PARTNER_MOM AS 
(
	SELECT tm_key_mth, product, partner_code, partner_name
		, ga_mtd_partner, prev_ga_mtd_partner, ga_mom_partner
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_partner DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_partner NULLS LAST, partner_name DESC) AS ga_bot_partner_rnk
		, m1_mtd_partner, prev_m1_mtd_partner, m1_mom_partner
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_partner DESC NULLS LAST, partner_name) AS m1_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_partner NULLS LAST, partner_name DESC) AS m1_bot_partner_rnk
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name
			, ga_mtd_partner, prev_ga_mtd_partner
			, CASE 	WHEN prev_ga_mtd_partner <> 0 
					THEN (ga_mtd_partner - prev_ga_mtd_partner) / prev_ga_mtd_partner * 100 
					END ga_mom_partner
			, m1_mtd_partner, prev_m1_mtd_partner
			, CASE 	WHEN prev_m1_mtd_partner <> 0 
					THEN (m1_mtd_partner - prev_m1_mtd_partner) / prev_m1_mtd_partner * 100 
					END m1_mom_partner
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name
				, ga_mtd_partner
				, LAG(ga_mtd_partner) OVER (PARTITION BY partner_code ORDER BY tm_key_mth) AS prev_ga_mtd_partner
				, m1_mtd_partner
				, LAG(m1_mtd_partner) OVER (PARTITION BY partner_code ORDER BY tm_key_mth) AS prev_m1_mtd_partner
			FROM (
				SELECT tm_key_mth, product, partner_code, partner_name
					, SUM(ga) AS ga_mtd_partner
					, SUM(m1) AS m1_mtd_partner
				FROM W_PREPAID
				WHERE mom_flag = 'Y'
				GROUP BY tm_key_mth, product, partner_code, partner_name
			) T1
		) T2
	) T3
	WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
	AND ga_mtd_partner <> 0
	-- AND m1_mtd_partner <> 0
) --> W_TXN_PARTNER_MOM

-- SELECT tm_key_mth, product, partner_code, partner_name
-- 	-- , ga_mtd_partner, prev_ga_mtd_partner, ga_mom_partner, ga_top_partner_rnk, ga_bot_partner_rnk
-- 	, m1_mtd_partner--, prev_m1_mtd_partner, m1_mom_partner
-- 	, m1_top_partner_rnk, m1_bot_partner_rnk
-- FROM W_TXN_PARTNER_MOM
-- WHERE (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3)
-- -- WHERE (m1_top_partner_rnk <= 3 OR m1_bot_partner_rnk <= 3)
-- -- ORDER BY ga_mom_partner DESC NULLS LAST, ga_mtd_partner DESC NULLS LAST
-- -- ORDER BY ga_mtd_partner DESC NULLS LAST, partner_name
-- ORDER BY ga_top_partner_rnk
-- -- ORDER BY m1_top_partner_rnk
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, partner_code, partner_name, group_sim
		, ga_mtd, prev_ga_mtd
		, CASE 	WHEN prev_ga_mtd <> 0 
				THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 
				END ga_mom
		, m1_mtd, prev_m1_mtd
		, CASE 	WHEN prev_m1_mtd <> 0 
				THEN (m1_mtd - prev_m1_mtd) / prev_m1_mtd * 100 
				END m1_mom
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name, group_sim
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY partner_code, group_sim ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY partner_code, group_sim ORDER BY tm_key_mth) AS prev_m1_mtd
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name, group_sim
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID A
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, partner_code, partner_name, group_sim
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-- ORDER BY ga_mtd DESC
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.partner_code, A.partner_name, A.group_sim
		, A.ga_mtd, B.ga_mom, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, B.m1_mom, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name, group_sim
			, ga_mtd, m1_mtd
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name, group_sim
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID
			WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
			GROUP BY tm_key_mth, product, partner_code, partner_name, group_sim
		) T1
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.partner_code = A.partner_code
		AND B.group_sim = A.group_sim
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-- ORDER BY ga_mtd DESC, partner_name
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT tm_key_mth, product, partner_code, partner_name, group_sim
	, ga_mtd, ga_mom, ga_mtd_cal, prev_ga_mtd, ga_mtd_partner, ga_top_partner_rnk, ga_bot_partner_rnk
	-- , m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd, m1_mtd_partner, m1_top_partner_rnk, m1_bot_partner_rnk

FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name, 'ALL' AS group_sim
		-- Gross Adds
		, ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_mtd_cal, prev_ga_mtd
		, NULL AS ga_mtd_partner, NULL AS ga_top_partner_rnk, NULL AS ga_bot_partner_rnk
		-- Inflow M1
		, m1_mtd
		, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
		, m1_mtd_cal, prev_m1_mtd
		, NULL AS m1_mtd_partner, NULL AS m1_top_partner_rnk, NULL AS m1_bot_partner_rnk
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
	
	SELECT A.tm_key_mth, A.product, A.partner_code, A.partner_name, A.group_sim
		, ga_mtd, ga_mom, ga_mtd_cal, prev_ga_mtd, C.ga_mtd_partner, C.ga_top_partner_rnk, C.ga_bot_partner_rnk
		, m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd, C.m1_mtd_partner, C.m1_top_partner_rnk, C.m1_bot_partner_rnk
	FROM W_TXN_MTD A
	INNER JOIN W_TXN_PARTNER_MOM C
		ON C.tm_key_mth = A.tm_key_mth
		AND C.partner_code = A.partner_code
		AND (C.ga_top_partner_rnk <= 3 OR C.ga_bot_partner_rnk <= 3)
) MTD_SUMMARY

ORDER BY tm_key_mth, product, ga_top_partner_rnk, ga_mtd DESC, partner_name, group_sim