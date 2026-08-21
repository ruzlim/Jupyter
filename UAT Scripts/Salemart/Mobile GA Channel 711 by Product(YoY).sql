

/*** Mobile GA Channel 711 by Product(YoY) ***/

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


WITH W_PARAM AS 
(
	SELECT prev_yr, curr_yr, mth, day
        , CAST(prev_yr || '0101' AS INT) AS prev_start_date
        , CAST(prev_yr || mth || day AS INT) AS prev_end_date
        , CAST(curr_yr || '0101' AS INT) AS curr_start_date
        , CAST(curr_yr || mth || day AS INT) AS curr_end_date
	FROM (
		SELECT '2025' AS prev_yr
			, '2026' AS curr_yr
			, '08' AS mth
			, '02' AS day
	) TMP
) --> W_PARAM

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
	WHERE team_code <> 'ไม่ระบุ' AND remark <> 'dummy'
	AND hop_hint = 'CHIANG MAI 1'
) --> W_ORG

-- SELECT * FROM W_ORG ORDER BY ccaatt
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID_YTD AS 
(
    SELECT SUBSTRING(tm_key_day,1,4) AS tm_key_yr, product, company
        , SUM(activation) AS ga_ytd
        , SUM(activation_value) AS m1_ytd
    FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
    CROSS JOIN W_PARAM P 
    WHERE (a.tm_key_day BETWEEN p.curr_start_date AND p.curr_end_date 
        OR a.tm_key_day BETWEEN p.prev_start_date AND p.prev_end_date)
    AND sub_product IN ('PREPAY', 'INFLOW_M1')
	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
    AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
    GROUP BY SUBSTRING(tm_key_day,1,4), product, company
) --> W_PREPAID_YTD

-- SELECT * FROM W_PREPAID_YTD
-----------------------------------------------------------------------------------------------------------------------


, W_POSTPAID_YTD AS 
(
	SELECT SUBSTRING(tm_key_day,1,4) AS tm_key_yr, product, company--, sub_product
		, SUM(activation) AS ga_ytd
		, SUM(activation_value) AS m1_ytd
	FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_POSTPAID_DAY A 
	CROSS JOIN W_PARAM P 
    WHERE (a.tm_key_day BETWEEN p.curr_start_date AND p.curr_end_date 
        OR a.tm_key_day BETWEEN p.prev_start_date AND p.prev_end_date)
	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
	AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
	GROUP BY SUBSTRING(tm_key_day,1,4), product, company
) --> W_POSTPAID_YTD

-- SELECT * FROM W_POSTPAID_YTD
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD AS 
(
	SELECT tm_key_yr, product
		, SUM(ga_ytd) AS ga_ytd
		, SUM(m1_ytd) AS m1_ytd
	FROM (
		SELECT * FROM W_PREPAID_YTD
		UNION ALL 
		SELECT * FROM W_POSTPAID_YTD
	) MB
	GROUP BY tm_key_yr, product
) --> W_TXN_YTD

-- SELECT * FROM W_TXN_YTD
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_COMP AS 
(
	SELECT tm_key_yr, product, company, ga_ytd, m1_ytd
	FROM (
		SELECT * FROM W_PREPAID_YTD
		UNION ALL 
		SELECT * FROM W_POSTPAID_YTD
	) MB
) --> W_TXN_YTD_BY_COMP

-- SELECT * FROM W_TXN_YTD_BY_COMP
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product

SELECT tm_key_yr, product
	, ga_ytd, prev_ga_ytd, ga_yoy
	, m1_ytd, prev_m1_ytd, m1_yoy
	, arpu_ytd, prev_arpu_ytd
	, CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
FROM (
	SELECT tm_key_yr, product
		, ga_ytd, prev_ga_ytd
		, CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
		, m1_ytd, prev_m1_ytd
		, CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
		, arpu_ytd
		, LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_arpu_ytd
	FROM (
		SELECT tm_key_yr, product
			, ga_ytd
			, LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_ga_ytd
			, m1_ytd
			, LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_m1_ytd
			, CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
		FROM (
			SELECT tm_key_yr, 'ALL' AS product
				, SUM(ga_ytd) AS ga_ytd
				, SUM(m1_ytd) AS m1_ytd
			FROM W_TXN_YTD AS TOTAL_YTD
			GROUP BY tm_key_yr

			UNION ALL 

			SELECT * FROM W_TXN_YTD	
		) T
	) T1
) T2

WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)

ORDER BY tm_key_yr, product
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, company

SELECT tm_key_yr, product, company
	, ga_ytd, prev_ga_ytd, ga_yoy
	, m1_ytd, prev_m1_ytd, m1_yoy
	, arpu_ytd, prev_arpu_ytd
	, CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
FROM (
	SELECT tm_key_yr, product, company
		, ga_ytd, prev_ga_ytd
		, CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
		, m1_ytd, prev_m1_ytd
		, CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
		, arpu_ytd
		, LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product, company ORDER BY tm_key_yr) AS prev_arpu_ytd
	FROM (
		SELECT tm_key_yr, product, company
			, ga_ytd
			, LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, company ORDER BY tm_key_yr) AS prev_ga_ytd
			, m1_ytd
			, LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, company ORDER BY tm_key_yr) AS prev_m1_ytd
			, CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
		FROM (
			SELECT tm_key_yr, product, 'ALL' AS company
				, SUM(ga_ytd) AS ga_ytd
				, SUM(m1_ytd) AS m1_ytd
			FROM W_TXN_YTD AS TOTAL_YTD
			GROUP BY tm_key_yr, product

			UNION ALL 

			SELECT * FROM W_TXN_YTD_BY_COMP	
		) T
	) T1
) T2

WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)

ORDER BY tm_key_yr, product, company