/*** 
	Prepaid GA & ARPU by PBH, SKU(MoM) 

		Test Case: 11454
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
			20260501::INTEGER AS p_start_date, 20260630::INTEGER AS p_end_date 
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
	-- AND district_en = 'Mueang Chiang Mai'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, orgid_h, hop_hint, group_sim
		, ga, m1
		, CASE WHEN COALESCE(ga,0) <> 0 THEN m1/ga END arpu
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, group_sim--, gp_sku
			, O.orgid_h, O.hop_hint
			, SUM(activation) AS ga
			, SUM(activation_value) AS m1
		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
		INNER JOIN W_ORG O 
			ON O.ccaatt = A.partner_ccaatt
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
		AND sub_product IN ('PREPAY', 'INFLOW_M1')
		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, group_sim, O.orgid_h, O.hop_hint
	) TMP
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-- -- ORDER by tm_key_day, hop_hint, group_sim
-- ORDER by tm_key_day, ga DESC, group_sim
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_PBH_MOM AS 
(
	SELECT tm_key_mth, product, orgid_h, hop_hint
		, ga_mtd_pbh, prev_ga_mtd_pbh, ga_mom_pbh
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_pbh DESC NULLS LAST, hop_hint) AS ga_top_pbh_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY ga_mtd_pbh NULLS LAST, hop_hint) AS ga_bot_pbh_rnk
		, m1_mtd_pbh, prev_m1_mtd_pbh, m1_mom_pbh
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_pbh DESC NULLS LAST, hop_hint) AS m1_top_pbh_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY m1_mtd_pbh NULLS LAST, hop_hint) AS m1_bot_pbh_rnk
		, arpu_mtd_pbh, prev_arpu_mtd_pbh, arpu_mom_pbh
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY arpu_mtd_pbh DESC NULLS LAST, hop_hint) AS arpu_top_pbh_rnk
		, DENSE_RANK() OVER (PARTITION BY tm_key_mth ORDER BY arpu_mtd_pbh NULLS LAST, hop_hint) AS arpu_bot_pbh_rnk
	FROM (
		SELECT tm_key_mth, product, orgid_h, hop_hint
			, ga_mtd_pbh, prev_ga_mtd_pbh
			, CASE 	WHEN prev_ga_mtd_pbh <> 0 
					THEN (ga_mtd_pbh - prev_ga_mtd_pbh) / prev_ga_mtd_pbh * 100 
					END ga_mom_pbh
			, m1_mtd_pbh, prev_m1_mtd_pbh
			, CASE 	WHEN prev_m1_mtd_pbh <> 0 
					THEN (m1_mtd_pbh - prev_m1_mtd_pbh) / prev_m1_mtd_pbh * 100 
					END m1_mom_pbh
			, arpu_mtd_pbh, prev_arpu_mtd_pbh
			, CASE 	WHEN prev_arpu_mtd_pbh <> 0 
					THEN (arpu_mtd_pbh - prev_arpu_mtd_pbh) / prev_arpu_mtd_pbh * 100 
					END arpu_mom_pbh
		FROM (
			SELECT tm_key_mth, product, orgid_h, hop_hint
				, ga_mtd_pbh
				, LAG(ga_mtd_pbh) OVER (PARTITION BY orgid_h ORDER BY tm_key_mth) AS prev_ga_mtd_pbh
				, m1_mtd_pbh
				, LAG(m1_mtd_pbh) OVER (PARTITION BY orgid_h ORDER BY tm_key_mth) AS prev_m1_mtd_pbh
				, arpu_mtd_pbh
				, LAG(arpu_mtd_pbh) OVER (PARTITION BY orgid_h ORDER BY tm_key_mth) AS prev_arpu_mtd_pbh
			FROM (
				SELECT tm_key_mth, product, orgid_h, hop_hint
					, SUM(ga) AS ga_mtd_pbh
					, SUM(m1) AS m1_mtd_pbh
					, CASE WHEN COALESCE(SUM(ga),0) <> 0 THEN SUM(m1)/SUM(ga) END arpu_mtd_pbh
				FROM W_PREPAID
				WHERE mom_flag = 'Y'
				GROUP BY tm_key_mth, product, orgid_h, hop_hint
			) T1
		) T2
		WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
	) T3
) --> W_TXN_PBH_MOM

-- SELECT * FROM W_TXN_PBH_MOM
-- ORDER BY tm_key_mth, ga_top_pbh_rnk
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, orgid_h, hop_hint, group_sim
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
		SELECT tm_key_mth, product, orgid_h, hop_hint, group_sim
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY orgid_h, group_sim ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY orgid_h, group_sim ORDER BY tm_key_mth) AS prev_m1_mtd
			, arpu_mtd
			, LAG(arpu_mtd) OVER (PARTITION BY orgid_h, group_sim ORDER BY tm_key_mth) AS prev_arpu_mtd
		FROM (
			SELECT tm_key_mth, product, orgid_h, hop_hint, group_sim
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
				, CASE WHEN COALESCE(SUM(ga),0) <> 0 THEN SUM(m1)/SUM(ga) END arpu_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, orgid_h, hop_hint, group_sim
		) T1
	) T2
	WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-- ORDER BY tm_key_mth, hop_hint, ga_mtd DESC
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.orgid_h, A.hop_hint, A.group_sim
		, A.ga_mtd, B.ga_mom, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, B.m1_mom, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
		, A.arpu_mtd, B.arpu_mom, B.arpu_mtd AS arpu_mtd_cal, B.prev_arpu_mtd
	FROM (
		SELECT tm_key_mth, product, orgid_h, hop_hint, group_sim
			, SUM(ga) AS ga_mtd
			, SUM(m1) AS m1_mtd
			, CASE WHEN COALESCE(SUM(ga),0) <> 0 THEN SUM(m1)/SUM(ga) END arpu_mtd
		FROM W_PREPAID
		GROUP BY tm_key_mth, product, orgid_h, hop_hint, group_sim
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.orgid_h = A.orgid_h
		AND B.group_sim = A.group_sim
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-- ORDER BY tm_key_mth, hop_hint, ga_mtd DESC
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary

SELECT tm_key_mth, product, orgid_h, hop_hint, group_sim
	, ga_mtd_pbh, ga_top_pbh_rnk, ga_mtd, ga_mom--, ga_mtd_cal, prev_ga_mtd
	-- , m1_mtd_pbh, m1_top_pbh_rnk, m1_mtd, m1_mom--, m1_mtd_cal, prev_m1_mtd
	, arpu_mtd_pbh, arpu_top_pbh_rnk, arpu_mtd, arpu_mom--, arpu_mtd_cal, prev_arpu_mtd
FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS orgid_h, 'ALL' AS hop_hint, 'ALL' AS group_sim
		-- Gross Adds
		, ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_mtd_cal, prev_ga_mtd
		, NULL AS ga_mtd_pbh, NULL AS ga_top_pbh_rnk, NULL AS ga_bot_pbh_rnk
		-- Inflow M1
		, m1_mtd
		, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
		, m1_mtd_cal, prev_m1_mtd
		, NULL AS m1_mtd_pbh, NULL AS m1_top_pbh_rnk, NULL AS m1_bot_pbh_rnk
		--ARPU 
		, arpu_mtd
		, CASE WHEN prev_arpu_mtd <> 0 THEN (arpu_mtd_cal - prev_arpu_mtd) / prev_arpu_mtd * 100 END arpu_mom
		, arpu_mtd_cal, prev_arpu_mtd
		, NULL AS arpu_mtd_pbh, NULL AS arpu_top_pbh_rnk, NULL AS arpu_bot_pbh_rnk
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
	
	SELECT A.tm_key_mth, A.product, A.orgid_h, A.hop_hint, A.group_sim
		, A.ga_mtd, A.ga_mom, A.ga_mtd_cal, A.prev_ga_mtd, B.ga_mtd_pbh, B.ga_top_pbh_rnk, B.ga_bot_pbh_rnk
		, A.m1_mtd, A.m1_mom, A.m1_mtd_cal, A.prev_m1_mtd, B.m1_mtd_pbh, B.m1_top_pbh_rnk, B.m1_bot_pbh_rnk
		, A.arpu_mtd, A.arpu_mom, A.arpu_mtd_cal, A.prev_arpu_mtd, B.arpu_mtd_pbh, B.arpu_top_pbh_rnk, B.arpu_bot_pbh_rnk
	FROM W_TXN_MTD A
	INNER JOIN W_TXN_PBH_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.orgid_h = A.orgid_h
		-- AND (B.ga_top_pbh_rnk <= 3 OR B.ga_bot_pbh_rnk <= 3)
) MTD_SUMMARY

-- ORDER BY tm_key_mth, product, hop_hint, group_sim
ORDER BY tm_key_mth, product, ga_top_pbh_rnk, group_sim