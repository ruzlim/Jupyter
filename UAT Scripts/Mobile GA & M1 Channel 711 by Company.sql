

/*** Mobile GA & M1 Channel 711 by Company ***/

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
	AND hop_hint = 'CHIANG MAI 1'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
    SELECT tm_key_mth, tm_key_day, product, company
        , SUM(activation) AS ga
        , SUM(activation_value) AS m1
    FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
    CROSS JOIN W_PARAM P 
    WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
    AND sub_product IN ('PREPAY', 'INFLOW_M1')
	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
    AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
    GROUP BY tm_key_mth, tm_key_day, product, company
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-----------------------------------------------------------------------------------------------------------------------


, W_POSTPAID AS 
(
	SELECT tm_key_mth, tm_key_day, product, company
		, SUM(activation) AS ga
		, SUM(activation_value) AS m1
	FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_POSTPAID_DAY A 
    CROSS JOIN W_PARAM P 
    WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
	AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
	GROUP BY tm_key_mth, tm_key_day, product, company
) --> W_POSTPAID

-- SELECT * FROM W_POSTPAID
-----------------------------------------------------------------------------------------------------------------------


, W_MOBILE AS 
(
	SELECT tm_key_mth, tm_key_day
		, product
		, SUM(ga) AS ga
		, SUM(m1) AS m1
	FROM (
		SELECT * FROM W_PREPAID
		UNION ALL 
		SELECT * FROM W_POSTPAID
	) MB
	GROUP BY tm_key_mth, tm_key_day, product
) --> W_MOBILE

-- SELECT * FROM W_MOBILE
-- ORDER BY tm_key_day, product
-----------------------------------------------------------------------------------------------------------------------


, W_MOBILE_BY_COMP AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, company, ga, m1
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, company, ga, m1
		FROM (
			SELECT * FROM W_PREPAID
			UNION ALL 
			SELECT * FROM W_POSTPAID
		) A
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
	) MB
) --> W_MOBILE_BY_COMP

-- SELECT * FROM W_MOBILE_BY_COMP
-- ORDER BY tm_key_day, product, company
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, company
		, ga_mtd, prev_ga_mtd
		, CASE 	WHEN prev_ga_mtd <> 0 
				THEN (ga_mtd - prev_ga_mtd) / prev_ga_mtd * 100 
				END ga_mom
		, m1_mtd, prev_m1_mtd
		, CASE 	WHEN prev_m1_mtd <> 0 
				THEN (m1_mtd - prev_m1_mtd) / prev_m1_mtd * 100 
				END m1_mom
	FROM (
		SELECT tm_key_mth, product, company
			, ga_mtd
			, LAG(ga_mtd) OVER (PARTITION BY company ORDER BY tm_key_mth) AS prev_ga_mtd
			, m1_mtd
			, LAG(m1_mtd) OVER (PARTITION BY company ORDER BY tm_key_mth) AS prev_m1_mtd
		FROM (
			SELECT tm_key_mth, product, company
				, SUM(ga) AS ga_mtd
				, SUM(m1) AS m1_mtd
			FROM W_MOBILE_BY_COMP
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, company
		) T1
	) T2
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-- ORDER BY tm_key_mth, product, company
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.company
		, A.ga_mtd, B.ga_mom, B.ga_mtd AS ga_mtd_cal, B.prev_ga_mtd
		, A.m1_mtd, B.m1_mom, B.m1_mtd AS m1_mtd_cal, B.prev_m1_mtd
	FROM (
		SELECT tm_key_mth, product, company
			, SUM(ga) AS ga_mtd
			, SUM(m1) AS m1_mtd
		FROM W_PREPAID
		GROUP BY tm_key_mth, product, company
	) A
	INNER JOIN W_TXN_MOM B
		ON B.tm_key_mth = A.tm_key_mth
		AND B.product = A.product
		AND B.company = A.company
) --> W_TXN_MTD

-- SELECT * FROM W_TXN_MTD
-- ORDER BY tm_key_mth, product, company
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> MTD Summary by company

SELECT tm_key_mth, product, company
	, ga_mtd, ga_mom, ga_mtd_cal, prev_ga_mtd
	-- , m1_mtd, m1_mom, m1_mtd_cal, prev_m1_mtd

FROM (
	SELECT tm_key_mth, 'ALL' AS product, 'ALL' AS company
		-- Gross Adds
		, ga_mtd
		, CASE WHEN prev_ga_mtd <> 0 THEN (ga_mtd_cal - prev_ga_mtd) / prev_ga_mtd * 100 END ga_mom
		, ga_mtd_cal, prev_ga_mtd
		-- Inflow M1
		, m1_mtd
		, CASE WHEN prev_m1_mtd <> 0 THEN (m1_mtd_cal - prev_m1_mtd) / prev_m1_mtd * 100 END m1_mom
		, m1_mtd_cal, prev_m1_mtd
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
	
	SELECT * FROM W_TXN_MTD A
) MTD_SUMMARY

WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)

ORDER BY tm_key_mth, product, company