

/*** Prepaid GA Channel 7-11 by Partner(MoM, Ach) ***/

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


WITH W_PARAM AS
(
    SELECT p_start_date, p_end_date
        , SUBSTRING(p_end_date, 7, 2)::INT AS mom_day
        , SUBSTRING(p_end_date, 1, 6)::INT AS curr_tm_key_mth
	FROM ( 
		SELECT 
			20260802::INTEGER AS p_start_date, 20260802::INTEGER AS p_end_date 
			-- 20260515::INTEGER AS p_start_date, 20260520::INTEGER AS p_end_date 
			-- 20260105::INTEGER AS p_start_date, 20260105::INTEGER AS p_end_date 
			-- 20250601::INTEGER AS p_start_date, 20250731::INTEGER AS p_end_date 
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

-- SELECT * FROM W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(

	SELECT tm_key_mth, tm_key_day, day, days_in_month, mom_flag, product, partner_code, partner_name
		, ga, ga_target_mth, ga_target
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga DESC NULLS LAST, partner_name) AS ga_top_partner_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_day ORDER BY ga NULLS LAST, partner_name) AS ga_bot_partner_rnk
	FROM (
		SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
			, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
			, product, partner_code, partner_name
			, ga, ga_target_mth
			, SUM(ga_target_mth) OVER (PARTITION BY tm_key_mth, partner_code) / days_in_month AS ga_target
		FROM (
			SELECT tm_key_mth, tm_key_day
				, SUBSTRING(tm_key_day, 7, 2)::INT AS day
				, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
				, P.mom_day
				, product--, group_channel, tds_special_channel--, group_sub_channel
				-- , group_sim--, gp_sku
				, partner_code, partner_name
				, SUM(activation) AS ga
				, SUM(target_ga) AS ga_target_mth
			FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
			CROSS JOIN W_PARAM P 
			WHERE A.tm_key_day BETWEEN P.P_START_DATE AND P.P_END_DATE
			AND sub_product = 'PREPAY'
			AND group_sim IN ('MASS', 'MIGRANT')
			AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
			AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
			GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, partner_code, partner_name
		) T1
	) T2
	WHERE ga <> 0
) --> W_PREPAID

SELECT * FROM W_PREPAID
WHERE (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3)
ORDER BY tm_key_day, ga_top_partner_rnk
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, partner_code, partner_name
		, ga_mtd, prev_ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY partner_code ORDER BY tm_key_mth) AS prev_ga_mtd
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name
				, SUM(ga) AS ga_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, partner_code, partner_name
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.partner_code, A.partner_name
		, A.ga_mtd, A.ga_ach, B.ga_mom
		, A.ga_target_mtd
		, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
	FROM (
		SELECT tm_key_mth, product, partner_code, partner_name
			, ga_mtd, ga_target_mtd
			, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
		FROM (
			SELECT tm_key_mth, product, partner_code, partner_name
				, SUM(ga) AS ga_mtd
				, SUM(ga_target) AS ga_target_mtd
			FROM W_PREPAID
			GROUP BY tm_key_mth, product, partner_code, partner_name
		) T1
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.partner_code = A.partner_code
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT *
FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name
		-- Gross Adds
		, ga_mtd
		, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_target_mtd, ga_mtd_cal, prev_ga_mtd
	FROM (
		SELECT tm_key_mth
			, SUM(ga_mtd) AS ga_mtd
			, SUM(ga_target_mtd) AS ga_target_mtd
			, SUM(ga_mtd_cal) AS ga_mtd_cal
			, SUM(prev_ga_mtd) AS prev_ga_mtd
		FROM W_TXN_MTD
		GROUP BY tm_key_mth
	) TOTAL_MTD
	
	UNION ALL 
	
	SELECT * FROM W_TXN_MTD
) MTD_SUMMARY

WHERE tm_key_mth = (select curr_tm_key_mth from W_PARAM)

ORDER BY tm_key_mth, product, ga_ach DESC NULLS LAST, ga_mtd DESC