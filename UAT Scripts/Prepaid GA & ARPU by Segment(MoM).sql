

/*** Prepaid GA & ARPU by Segment(MoM) ***/

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


WITH W_PARAM AS
(
    SELECT p_start_date, p_end_date, SUBSTRING(p_end_date, 7, 2)::INT AS mom_day
	FROM ( 
		SELECT 
			20260201::INTEGER AS p_start_date, 20260312::INTEGER AS p_end_date 
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
	AND PROVINCE_ENG = 'Chiang Mai'
	-- AND DISTRICT_EN = 'Mueang Chiang Mai'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, group_sim
		, ga, m1
		, CASE WHEN COALESCE(ga,0) <> 0 THEN m1/ga END arpu
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, group_sim--, gp_sku
			, SUM(activation) AS ga
			, SUM(activation_value) AS m1
		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.P_START_DATE AND P.P_END_DATE
		AND sub_product IN ('PREPAY', 'INFLOW_M1')
		AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, group_sim
	) TMP
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, group_sim
		, ga_mtd, prev_ga_mtd
		, CASE 	WHEN prev_ga_mtd <> 0 
				THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 
				END ga_mom
		, m1_mtd, prev_m1_mtd
		, CASE 	WHEN prev_m1_mtd <> 0 
				THEN (m1_mtd - prev_m1_mtd) / prev_m1_mtd * 100 
				END m1_mom
		, arpu_mtd, prev_arpu_mtd
		, CASE 	WHEN prev_arpu_mtd <> 0 
				THEN (arpu_mtd - prev_arpu_mtd) / prev_arpu_mtd * 100 
				END arpu_mom
	FROM (
		SELECT tm_key_mth, product, group_sim
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY group_sim ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY group_sim ORDER BY tm_key_mth) AS prev_m1_mtd
			, CASE WHEN COALESCE(ga_mtd,0) <> 0 THEN m1_mtd/ga_mtd END arpu_mtd
			, LAG(CASE WHEN COALESCE(ga_mtd,0) <> 0 THEN m1_mtd/ga_mtd END) OVER (PARTITION BY group_sim ORDER BY tm_key_mth) AS prev_arpu_mtd
		FROM (
			SELECT tm_key_mth, product, group_sim
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, group_sim
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.group_sim
		, A.ga_mtd, B.ga_mom, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, B.m1_mom, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
		, A.arpu_mtd, B.arpu_mom, B.arpu_mtd AS arpu_mtd_cal, B.prev_arpu_mtd
	FROM (
		SELECT tm_key_mth, product, group_sim
			, ga_mtd
			, m1_mtd
			, CASE WHEN COALESCE(ga_mtd,0) <> 0 THEN m1_mtd/ga_mtd END arpu_mtd
		FROM (
			SELECT tm_key_mth, product, group_sim
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_PREPAID
			GROUP BY tm_key_mth, product, group_sim
		) T1
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.group_sim = A.group_sim
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT *
FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS group_sim
		-- Gross Adds
		, ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_mtd_cal, prev_ga_mtd
		-- Inflow M1
		, m1_mtd
		, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
		, m1_mtd_cal, prev_m1_mtd
		--ARPU 
		, arpu_mtd
		, CASE WHEN prev_arpu_mtd <> 0 THEN (arpu_mtd_cal - prev_arpu_mtd) / prev_arpu_mtd * 100 END arpu_mom
		, arpu_mtd_cal, prev_arpu_mtd
	FROM (
		SELECT tm_key_mth
			, SUM(ga_mtd) AS ga_mtd
			, SUM(ga_mtd_cal) AS ga_mtd_cal
			, SUM(prev_ga_mtd) AS prev_ga_mtd
			, SUM(m1_mtd) AS m1_mtd
			, SUM(m1_mtd_cal) AS m1_mtd_cal
			, SUM(prev_m1_mtd) AS prev_m1_mtd
			, SUM(m1_mtd) / SUM(ga_mtd) AS arpu_mtd
			, SUM(m1_mtd_cal) / SUM(ga_mtd_cal) AS arpu_mtd_cal
			, SUM(prev_m1_mtd) / SUM(prev_ga_mtd) AS prev_arpu_mtd
		FROM W_TXN_MTD
		GROUP BY tm_key_mth
	) TOTAL_MTD
	
	UNION ALL 
	
	SELECT * FROM W_TXN_MTD
) MTD_SUMMARY

ORDER BY tm_key_mth, product, group_sim--, m1_ach DESC