/*** Prepaid GA & M1 by Channel(MoM, Ach) ***/

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
			-- 20260501::INTEGER AS p_start_date, 20260607::INTEGER AS p_end_date 
			-- 20260501::INTEGER AS p_start_date, 20260603::INTEGER AS p_end_date 
			20251201::INTEGER AS p_start_date, 20260131::INTEGER AS p_end_date 
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
	-- AND d_cluster LIKE 'CHIANG MAI%'
	-- AND province_eng = 'Chiang Mai'
	AND district_en = 'Mueang Chiang Mai'
) --> W_ORG

-- SELECT * FROM W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, group_channel, tds_special_channel
		, ga, ga_target_mth
		, SUM(ga_target_mth) OVER (PARTITION BY tm_key_mth, group_channel, tds_special_channel) / days_in_month AS ga_target
		, m1, m1_target_mth
		, SUM(m1_target_mth) OVER (PARTITION BY tm_key_mth, group_channel, tds_special_channel) / days_in_month AS m1_target
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, group_channel, tds_special_channel--, group_sub_channel
			-- , group_sim--, gp_sku
			-- , partner_code, partner_name
			, SUM(activation) AS ga
			, SUM(target_ga) AS ga_target_mth
			, SUM(activation_value) AS m1
			, SUM(target_inflow_m1) AS m1_target_mth
		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
		AND sub_product IN ('PREPAY', 'INFLOW_M1')
		AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, group_channel, tds_special_channel
	) TMP
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, group_channel, tds_special_channel
		, ga_mtd, prev_ga_mtd
		, CASE 	WHEN prev_ga_mtd <> 0 
				THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 
				END ga_mom
		, m1_mtd, prev_m1_mtd
		, CASE 	WHEN prev_m1_mtd <> 0 
				THEN (m1_mtd - prev_m1_mtd) / prev_m1_mtd * 100 
				END m1_mom
	FROM (
		SELECT tm_key_mth, product, group_channel, tds_special_channel
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY group_channel, tds_special_channel ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY group_channel, tds_special_channel ORDER BY tm_key_mth) AS prev_m1_mtd
		FROM (
			SELECT tm_key_mth, product, group_channel, tds_special_channel
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, group_channel, tds_special_channel
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.group_channel, A.tds_special_channel
		, A.ga_mtd, A.ga_ach, B.ga_mom
		, A.ga_target_mtd
		, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, A.m1_ach, B.m1_mom
		, A.m1_target_mtd
		, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
	FROM (
		SELECT tm_key_mth, product, group_channel, tds_special_channel
			, ga_mtd, ga_target_mtd
			, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
			, m1_mtd, m1_target_mtd
			, CASE WHEN m1_target_mtd <> 0 THEN m1_mtd / m1_target_mtd * 100 END m1_ach
		FROM (
			SELECT tm_key_mth, product, group_channel, tds_special_channel
				, SUM(ga) AS ga_mtd
				, SUM(ga_target) AS ga_target_mtd
				, SUM(m1) AS m1_mtd
				, SUM(m1_target) AS m1_target_mtd
			FROM W_PREPAID
			WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
			GROUP BY tm_key_mth, product, group_channel, tds_special_channel
		) T1
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.group_channel = A.group_channel
		AND B.tds_special_channel = A.tds_special_channel
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT tm_key_mth, product, group_channel, tds_special_channel
	-- , ga_mtd, ga_ach, ga_mom, ga_target_mtd, ga_mtd_cal, prev_ga_mtd
	, m1_mtd, m1_ach, m1_mom, m1_target_mtd, m1_mtd_cal, prev_m1_mtd

FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS group_channel, 'ALL' AS tds_special_channel
		-- Gross Adds
		, ga_mtd
		, CASE WHEN ga_target_mtd <> 0 THEN ga_mtd / ga_target_mtd * 100 END ga_ach
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_target_mtd, ga_mtd_cal, prev_ga_mtd
		-- Inflow M1
		, m1_mtd
		, CASE WHEN m1_target_mtd <> 0 THEN m1_mtd / m1_target_mtd * 100 END m1_ach
		, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
		, m1_target_mtd, m1_mtd_cal, prev_m1_mtd
	FROM (
		SELECT tm_key_mth
			, SUM(ga_mtd) AS ga_mtd
			, SUM(ga_target_mtd) AS ga_target_mtd
			, SUM(ga_mtd_cal) AS ga_mtd_cal
			, SUM(prev_ga_mtd) AS prev_ga_mtd
			, SUM(m1_mtd) AS m1_mtd
			, SUM(m1_target_mtd) AS m1_target_mtd
			, SUM(m1_mtd_cal) AS m1_mtd_cal
			, SUM(prev_m1_mtd) AS prev_m1_mtd
		FROM W_TXN_MTD
		GROUP BY tm_key_mth
	) TOTAL_MTD
	
	UNION ALL 
	
	SELECT * FROM W_TXN_MTD
) MTD_SUMMARY

-- WHERE ((m1_mtd IS NOT NULL AND m1_mtd <> 0) AND m1_mom > 0) 
WHERE (m1_mtd IS NOT NULL AND m1_mtd <> 0)
-- WHERE m1_mtd > 0 --AND m1_mom > 0) 
OR product = 'ALL'

ORDER BY tm_key_mth, product
	-- , m1_ach DESC NULLS LAST
	-- , m1_mom DESC NULLS LAST, m1_mtd DESC NULLS LAST
	, m1_mtd DESC NULLS LAST