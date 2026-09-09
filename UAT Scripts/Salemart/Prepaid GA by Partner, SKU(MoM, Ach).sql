/*** 
	Prepaid GA by Partner, SKU(MoM, Ach) 

		Test Case: 11471
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
			-- 20260401::INTEGER AS p_start_date, 20260531::INTEGER AS p_end_date 
			20250917::INTEGER AS p_start_date, 20250917::INTEGER AS p_end_date 
			-- 20250801::INTEGER AS p_start_date, 20250917::INTEGER AS p_end_date 
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
	AND hop_hint = 'CHIANG MAI 1'
) --> W_ORG

-- SELECT * FROM W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month, mom_flag, product, partner_code, partner_name, gp_sku
		, ga, ga_target_mth, ga_target
        , ga_partner
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga_partner DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga_partner NULLS LAST, partner_name) AS ga_bot_partner_rnk
	FROM (
		SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
			, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
			, product, partner_code, partner_name, gp_sku
			, ga, ga_target_mth
			, SUM(ga_target_mth) OVER (PARTITION BY tm_key_mth, partner_code) / days_in_month AS ga_target
            , SUM(ga) OVER (PARTITION BY tm_key_day, partner_code) AS ga_partner
		FROM (
			SELECT tm_key_mth, tm_key_day
				, SUBSTRING(tm_key_day, 7, 2)::INT AS day
				, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
				, P.mom_day
				, product, partner_code, partner_name, gp_sku
				, SUM(activation) AS ga
				, SUM(target_ga) AS ga_target_mth
			FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
			CROSS JOIN W_PARAM P 
			WHERE A.tm_key_day BETWEEN P.P_START_DATE AND P.P_END_DATE
			AND sub_product = 'PREPAY'
			AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') AND partner_code LIKE '711%'
			-- AND group_channel LIKE '%Branded Retail%' --True Shop ?
			AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
			GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, partner_code, partner_name, gp_sku
		) T1
	) T2
	WHERE ga <> 0
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-- -- WHERE (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3)
-- ORDER BY tm_key_day, ga_top_partner_rnk, gp_sku
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
		, ga_mtd, prev_ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY partner_code, gp_sku ORDER BY tm_key_mth) AS prev_ga_mtd
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
				, SUM(ga) AS ga_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, partner_code, partner_name, gp_sku
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.partner_code, A.partner_name, A.gp_sku
		, A.ga_mtd, A.ga_ach, B.ga_mom
		, A.ga_target_mtd
		, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.ga_partner_mtd
		, DENSE_RANK() OVER (PARTITION BY A.tm_key_mth ORDER BY A.ga_partner_mtd DESC NULLS LAST, A.partner_name) AS ga_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY A.tm_key_mth ORDER BY A.ga_partner_mtd NULLS LAST, A.partner_name) AS ga_bot_partner_rnk
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
			, ga_mtd, ga_target_mtd
			, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
			, SUM(ga_mtd) OVER (PARTITION BY tm_key_mth, partner_code) AS ga_partner_mtd
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
				, SUM(ga) AS ga_mtd
				, SUM(ga_target) AS ga_target_mtd
			FROM W_PREPAID
			GROUP BY tm_key_mth, product, partner_code, partner_name, gp_sku
		) T1
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.partner_code = A.partner_code
		AND B.gp_sku = A.gp_sku
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT tm_key_mth, product, partner_code, partner_name
	, MASS, MIGRANT, TOURIST
	, ga_partner_mtd, ga_top_partner_rnk, ga_bot_partner_rnk
FROM (
	SELECT tm_key_mth, product, partner_code, partner_name
		, ga_partner_mtd, ga_top_partner_rnk, ga_bot_partner_rnk
		, SUM(CASE WHEN gp_sku = 'MASS' THEN ga_mtd END) AS MASS
		, SUM(CASE WHEN gp_sku = 'MIGRANT' THEN ga_mtd END) AS MIGRANT
		, SUM(CASE WHEN gp_sku = 'TOURIST' THEN ga_mtd END) AS TOURIST
	FROM (
		SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name, 'ALL' AS gp_sku
			-- Gross Adds
			, ga_mtd
			, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
			, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
			, ga_target_mtd, ga_mtd_cal, prev_ga_mtd
			, ga_partner_mtd
			, NULL AS ga_top_partner_rnk, NULL AS ga_bot_partner_rnk
		FROM (
			SELECT tm_key_mth
				, SUM(ga_mtd) AS ga_mtd
				, SUM(ga_target_mtd) AS ga_target_mtd
				, SUM(ga_mtd_cal) AS ga_mtd_cal
				, SUM(prev_ga_mtd) AS prev_ga_mtd
				, SUM(ga_mtd) AS ga_partner_mtd
			FROM W_TXN_MTD
			GROUP BY tm_key_mth
		) TOTAL_MTD
		
		UNION ALL 
		
		SELECT * FROM W_TXN_MTD
	) TMP

	WHERE tm_key_mth = (select curr_tm_key_mth from W_PARAM)

	GROUP BY tm_key_mth, product, partner_code, partner_name, ga_partner_mtd, ga_top_partner_rnk, ga_bot_partner_rnk
) MTD_SUMMARY

ORDER BY tm_key_mth, product, ga_top_partner_rnk



--> MTD Summary

-- SELECT *
-- FROM (
-- 	SELECT tm_key_mth, product, partner_code, partner_name, gp_sku
-- 		, ga_mtd, ga_ach, ga_mom, ga_target_mtd, ga_mtd_cal, prev_ga_mtd, ga_partner_mtd
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_partner_mtd DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
-- 		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_partner_mtd NULLS LAST, partner_name) AS ga_bot_partner_rnk
-- 	FROM (
-- 		SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name, 'ALL' AS gp_sku
-- 			-- Gross Adds
-- 			, ga_mtd
-- 			, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
-- 			, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
-- 			, ga_target_mtd, ga_mtd_cal, prev_ga_mtd
-- 			, NULL AS ga_partner_mtd
-- 		FROM (
-- 			SELECT tm_key_mth
-- 				, SUM(ga_mtd) AS ga_mtd
-- 				, SUM(ga_target_mtd) AS ga_target_mtd
-- 				, SUM(ga_mtd_cal) AS ga_mtd_cal
-- 				, SUM(prev_ga_mtd) AS prev_ga_mtd
-- 			FROM W_TXN_MTD
-- 			GROUP BY tm_key_mth
-- 		) TOTAL_MTD
		
-- 		UNION ALL 
		
-- 		SELECT * FROM W_TXN_MTD
-- 	) TMP

-- 	WHERE tm_key_mth = (select curr_tm_key_mth from W_PARAM)
-- ) MTD_SUMMARY

-- ORDER BY tm_key_mth, product, ga_top_partner_rnk