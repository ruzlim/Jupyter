/*** 
    Mobile GA & M1 & ARPU by Partner, SKU(YoY, Ach) 

        Test Case 2: 11480, 11484, 11491, 11481, 11483, 11486, 11485, 11493, 11487
***/

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
			, '03' AS mth
			, '22' AS day
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
    -- AND tds_sgmd = 'North'
	-- AND hop_hint = 'CHIANG MAI 1'
	AND d_cluster LIKE 'CHIANG MAI%'
) --> W_ORG

-- SELECT * FROM W_ORG ORDER BY ccaatt
-----------------------------------------------------------------------------------------------------------------------


, W_RAW_YTD AS 
(
    -- Prepaid only
    SELECT SUBSTRING(tm_key_day,1,4) AS tm_key_yr
		, product, group_sim
		, group_channel||': '||tds_special_channel AS channel
		, group_channel, tds_special_channel, partner_code, partner_name
        , SUM(activation) AS ga_ytd
		, SUM(target_ga) AS ga_target_ytd
        , SUM(activation_value) AS m1_ytd
		, SUM(target_inflow_m1) AS m1_target_ytd
		, SUM(COALESCE(ap1d_1_49,0) + COALESCE(ap1d_50_99,0) + COALESCE(ap1d_100_119,0) + COALESCE(ap1d_120_149,0) + COALESCE(ap1d_150_199,0) + COALESCE(ap1d_200_249,0) + COALESCE(ap1d_250_299,0) + COALESCE(ap1d_300up,0)) AS ap1d_ytd
    FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
    CROSS JOIN W_PARAM P 
    WHERE (a.tm_key_day BETWEEN p.curr_start_date AND p.curr_end_date 
        OR a.tm_key_day BETWEEN p.prev_start_date AND p.prev_end_date)
    AND sub_product IN ('PREPAY', 'INFLOW_M1')
	-- AND group_sim IN ('MASS', 'MIGRANT')
	-- AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
	-- AND partner_code LIKE '711%'
    AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
    GROUP BY SUBSTRING(tm_key_day,1,4), product, group_sim, group_channel, tds_special_channel, partner_code, partner_name
) --> W_RAW_YTD

-- SELECT * FROM W_RAW_YTD
-----------------------------------------------------------------------------------------------------------------------


-- , W_RAW_YTD AS 
-- (
--     -- Prepaid
--     SELECT SUBSTRING(tm_key_day,1,4) AS tm_key_yr, product, partner_code, partner_name, group_sim
--         , SUM(activation) AS ga_ytd
--         , SUM(activation_value) AS m1_ytd
-- 		, SUM(target_ga) AS ga_target_ytd
--     FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
--     CROSS JOIN W_PARAM P 
--     WHERE (a.tm_key_day BETWEEN p.curr_start_date AND p.curr_end_date 
--         OR a.tm_key_day BETWEEN p.prev_start_date AND p.prev_end_date)
--     AND sub_product IN ('PREPAY', 'INFLOW_M1')
-- 	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
--     AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
--     GROUP BY SUBSTRING(tm_key_day,1,4), product, partner_code, partner_name, group_sim

--     UNION ALL 

--     -- Postpaid
-- 	SELECT SUBSTRING(tm_key_day,1,4) AS tm_key_yr, product--, company, sub_product
--         , partner_code, partner_name, gp_sku
-- 		, SUM(activation) AS ga_ytd
-- 		, SUM(activation_value) AS m1_ytd
--         , SUM(target) as ga_target_ytd
-- 	FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_POSTPAID_DAY A 
-- 	CROSS JOIN W_PARAM P 
--     WHERE (a.tm_key_day BETWEEN p.curr_start_date AND p.curr_end_date 
--         OR a.tm_key_day BETWEEN p.prev_start_date AND p.prev_end_date)
-- 	AND (tds_special_channel LIKE '7-Eleven%' OR tds_special_channel LIKE 'MT SYNERGY') 
-- 	AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
-- 	GROUP BY SUBSTRING(tm_key_day,1,4), product, partner_code, partner_name, gp_sku
-- ) --> W_RAW_YTD

-- -- SELECT * FROM W_RAW_YTD
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD AS 
(
	SELECT tm_key_yr, product
		, SUM(ga_ytd) AS ga_ytd
		, SUM(ga_target_ytd) AS ga_target_ytd
		, SUM(m1_ytd) AS m1_ytd
		, SUM(m1_target_ytd) AS m1_target_ytd
		, SUM(ap1d_ytd) AS ap1d_ytd
	FROM W_RAW_YTD
	GROUP BY tm_key_yr, product
) --> W_TXN_YTD

-- SELECT * FROM W_TXN_YTD
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_SKU AS 
(
	SELECT tm_key_yr, product, group_sim
		, SUM(ga_ytd) AS ga_ytd
		, SUM(ga_target_ytd) AS ga_target_ytd
		, SUM(m1_ytd) AS m1_ytd
		, SUM(m1_target_ytd) AS m1_target_ytd
		, SUM(ap1d_ytd) AS ap1d_ytd
	FROM W_RAW_YTD
	GROUP BY tm_key_yr, product, group_sim
) --> W_TXN_YTD_BY_SKU

-- SELECT * FROM W_TXN_YTD_BY_SKU

-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_CHANNEL AS 
(
	SELECT tm_key_yr, product, channel
		, ga_ytd, ga_target_ytd, prev_ga_ytd
		, CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
		, m1_ytd, m1_target_ytd, prev_m1_ytd
		, CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
		, ap1d_ytd
	FROM (
		SELECT tm_key_yr, product, channel
			, ga_ytd, ga_target_ytd
			, LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, channel ORDER BY tm_key_yr) AS prev_ga_ytd
			, m1_ytd, m1_target_ytd
			, LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, channel ORDER BY tm_key_yr) AS prev_m1_ytd
			, ap1d_ytd
		FROM (
			SELECT tm_key_yr, product--, group_channel, tds_special_channel
				, group_channel||': '||tds_special_channel AS channel
				, SUM(ga_ytd) AS ga_ytd
				, SUM(ga_target_ytd) AS ga_target_ytd
				, SUM(m1_ytd) AS m1_ytd
				, SUM(m1_target_ytd) AS m1_target_ytd
				, SUM(ap1d_ytd) AS ap1d_ytd
			FROM W_RAW_YTD
			GROUP BY tm_key_yr, product, group_channel, tds_special_channel
		) T1
	) T2
) --> W_TXN_YTD_BY_CHANNEL

-- SELECT * FROM W_TXN_YTD_BY_CHANNEL
-- WHERE m1_yoy < 0
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_CHANNEL_PARTNER AS 
(
	SELECT tm_key_yr, product, channel, partner_code, partner_name
		, SUM(ga_ytd) AS ga_ytd
		, SUM(ga_target_ytd) AS ga_target_ytd
		, SUM(m1_ytd) AS m1_ytd
		, SUM(m1_target_ytd) AS m1_target_ytd
		, SUM(ap1d_ytd) AS ap1d_ytd
	FROM W_RAW_YTD A
	GROUP BY tm_key_yr, product, channel, partner_code, partner_name
) --> W_TXN_YTD_BY_CHANNEL_PARTNER

-- SELECT * FROM W_TXN_YTD_BY_CHANNEL_PARTNER
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_PARTNER AS 
(
	SELECT tm_key_yr, product, partner_code, partner_name
		, SUM(ga_ytd) AS ga_ytd
		, SUM(ga_target_ytd) AS ga_target_ytd
		, SUM(m1_ytd) AS m1_ytd
		, SUM(m1_target_ytd) AS m1_target_ytd
		, SUM(ap1d_ytd) AS ap1d_ytd
	FROM W_RAW_YTD A
	GROUP BY tm_key_yr, product, partner_code, partner_name
) --> W_TXN_YTD_BY_PARTNER

-- SELECT * FROM W_TXN_YTD_BY_PARTNER
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_YTD_BY_PARTNER_SKU AS 
(
	SELECT tm_key_yr, product, partner_code, partner_name, group_sim
		, SUM(ga_ytd) AS ga_ytd
		, SUM(ga_target_ytd) AS ga_target_ytd
		, SUM(m1_ytd) AS m1_ytd
		, SUM(m1_target_ytd) AS m1_target_ytd
		, SUM(ap1d_ytd) AS ap1d_ytd
	FROM W_RAW_YTD
	GROUP BY tm_key_yr, product, partner_code, partner_name, group_sim
) --> W_TXN_YTD_BY_PARTNER_SKU

-- SELECT * FROM W_TXN_YTD_BY_PARTNER_SKU
-- WHERE tm_key_yr = 2026
-- ORDER BY tm_key_yr, product, partner_code, group_sim
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product

SELECT tm_key_yr, product
	-- , ga_ytd, ga_ach, ga_yoy--, prev_ga_ytd
	-- , m1_ytd, m1_ach, m1_yoy--, prev_m1_ytd
	, ap1d_ytd, ap1d_yoy, prev_ap1d_ytd
	-- , arpu_ytd
	-- , CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
	-- , prev_arpu_ytd
	
FROM (
	SELECT tm_key_yr, product
		, ga_ytd, ga_target_ytd, prev_ga_ytd
		, CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
		, CASE WHEN ga_target_ytd <> 0 THEN ga_ytd / ga_target_ytd * 100 END ga_ach
		, m1_ytd, m1_target_ytd, prev_m1_ytd
		, CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
		, CASE WHEN m1_target_ytd <> 0 THEN m1_ytd / m1_target_ytd * 100 END m1_ach
		, ap1d_ytd, prev_ap1d_ytd
		, CASE WHEN prev_ap1d_ytd <> 0 THEN (ap1d_ytd - prev_ap1d_ytd) / prev_ap1d_ytd * 100 END ap1d_yoy
		, arpu_ytd
		, LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_arpu_ytd
	FROM (
		SELECT tm_key_yr, product
			, ga_ytd, ga_target_ytd
			, LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_ga_ytd
			, m1_ytd, m1_target_ytd
			, LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_m1_ytd
			, ap1d_ytd
			, LAG(ap1d_ytd IGNORE NULLS) OVER (PARTITION BY product ORDER BY tm_key_yr) AS prev_ap1d_ytd
			, CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
		FROM (
			SELECT tm_key_yr, 'ALL' AS product
				, SUM(ga_ytd) AS ga_ytd
				, SUM(ga_target_ytd) AS ga_target_ytd
				, SUM(m1_ytd) AS m1_ytd
				, SUM(m1_target_ytd) AS m1_target_ytd
				, SUM(ap1d_ytd) AS ap1d_ytd
			FROM W_TXN_YTD AS TOTAL_YTD
			GROUP BY tm_key_yr

			UNION ALL 

			SELECT * FROM W_TXN_YTD	
            -- WHERE ga_ytd <> 0
		) T
	) T1
) T2

WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)

ORDER BY tm_key_yr, product

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, segment

-- SELECT tm_key_yr, product, group_sim
--     , ga_ytd, ga_yoy--, prev_ga_ytd, ga_ach
--     -- , m1_ytd
--     , arpu_ytd, arpu_yoy--, prev_arpu_ytd

-- FROM (
--     SELECT tm_key_yr, product, group_sim
--         , ga_ytd, ga_ach, ga_yoy, prev_ga_ytd
--         , m1_ytd
--         , arpu_ytd
--         , CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
--         , prev_arpu_ytd
--     FROM (
--         SELECT tm_key_yr, product, group_sim
--             , ga_ytd, prev_ga_ytd
--             , CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
--             , m1_ytd
--             , arpu_ytd
--             , LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product, group_sim ORDER BY tm_key_yr) AS prev_arpu_ytd
--             , ga_target_ytd
--             , CASE WHEN ga_target_ytd <> 0 THEN ga_ytd / ga_target_ytd * 100 END ga_ach
--         FROM (
--             SELECT tm_key_yr, product, group_sim
--                 , ga_ytd
--                 , LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, group_sim ORDER BY tm_key_yr) AS prev_ga_ytd
--                 , m1_ytd
--                 , CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
--                 , ga_target_ytd
--             FROM (
--                 SELECT tm_key_yr, 'ALL' AS product, 'ALL' AS group_sim
--                     , SUM(ga_ytd) AS ga_ytd
--                     , SUM(m1_ytd) AS m1_ytd
--                     , SUM(ga_target_ytd) AS ga_target_ytd
--                 FROM W_TXN_YTD_BY_SKU AS TOTAL_YTD
--                 GROUP BY tm_key_yr

--                 UNION ALL 

--                 SELECT * FROM W_TXN_YTD_BY_SKU
--                 WHERE ga_ytd <> 0
--             ) T
--         ) T1
--     ) T2
--     WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)
-- ) T3

-- ORDER BY tm_key_yr, product, group_sim

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, channel

-- SELECT tm_key_yr, product, channel
--     -- , ga_ytd, ga_yoy, ga_ach, ga_target_ytd--, prev_ga_ytd
--     , m1_ytd, m1_yoy, m1_ach, m1_target_ytd--, prev_m1_ytd

-- FROM (
--     SELECT tm_key_yr, product, channel
--         , ga_ytd, ga_target_ytd, ga_ach, prev_ga_ytd, ga_yoy
--         , m1_ytd, m1_target_ytd, m1_ach, prev_m1_ytd, m1_yoy
--     FROM (
--         SELECT tm_key_yr, product, channel
--             , ga_ytd, ga_target_ytd, ga_ach, prev_ga_ytd
--             , CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
--             , m1_ytd, m1_target_ytd, m1_ach, prev_m1_ytd
-- 			, CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
--         FROM (
--             SELECT tm_key_yr, product, channel
--                 , ga_ytd, ga_target_ytd
-- 				, CASE WHEN ga_target_ytd <> 0 THEN ga_ytd / ga_target_ytd * 100 END ga_ach
--                 , LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, channel ORDER BY tm_key_yr) AS prev_ga_ytd
--                 , m1_ytd, m1_target_ytd
-- 				, CASE WHEN m1_target_ytd <> 0 THEN m1_ytd / m1_target_ytd * 100 END m1_ach
-- 				, LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, channel ORDER BY tm_key_yr) AS prev_m1_ytd
--             FROM (
--                 SELECT tm_key_yr, 'ALL' AS product, 'ALL' AS channel
-- 					, SUM(ga_ytd) AS ga_ytd
-- 					, SUM(ga_target_ytd) AS ga_target_ytd
-- 					, SUM(m1_ytd) AS m1_ytd
-- 					, SUM(m1_target_ytd) AS m1_target_ytd
--                 FROM W_TXN_YTD_BY_CHANNEL AS TOTAL_YTD
--                 GROUP BY tm_key_yr

--                 UNION ALL 

-- 				SELECT tm_key_yr, product, channel
-- 					, ga_ytd, ga_target_ytd--, ga_yoy, prev_ga_ytd
-- 					, m1_ytd, m1_target_ytd--, m1_yoy, prev_m1_ytd
-- 				FROM W_TXN_YTD_BY_CHANNEL
--             ) T
--         ) T1
--     ) T2
--     WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)
-- ) T3

-- -- WHERE product = 'ALL' OR m1_yoy > 0
-- WHERE product = 'ALL' OR m1_ytd > 0

-- ORDER BY tm_key_yr, product, m1_ytd DESC

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, partner

-- SELECT tm_key_yr, product, partner_code, partner_name
--     , ga_ytd, ga_yoy--, ga_ach--, prev_ga_ytd
--     , ga_top_partner_rnk, ga_bot_partner_rnk
--     -- , m1_ytd, m1_yoy, prev_m1_ytd
--     -- , arpu_ytd, arpu_yoy, prev_arpu_ytd

-- FROM (
--     SELECT tm_key_yr, product, partner_code, partner_name
--         , ga_ytd, ga_ach, ga_yoy, prev_ga_ytd
--         , CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY ga_ytd DESC NULLS LAST, partner_name) END ga_top_partner_rnk
--         , CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY ga_ytd NULLS LAST, partner_name) END ga_bot_partner_rnk
--         , m1_ytd, m1_yoy, prev_m1_ytd
--         , arpu_ytd
--         , CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
--         , prev_arpu_ytd
--     FROM (
--         SELECT tm_key_yr, product, partner_code, partner_name
--             , ga_ytd, prev_ga_ytd
--             , CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
--             , m1_ytd, prev_m1_ytd
--             , CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
--             , arpu_ytd
--             , LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code ORDER BY tm_key_yr) AS prev_arpu_ytd
--             , ga_target_ytd
--             , CASE WHEN ga_target_ytd <> 0 THEN ga_ytd / ga_target_ytd * 100 END ga_ach
--         FROM (
--             SELECT tm_key_yr, product, partner_code, partner_name
--                 , ga_ytd
--                 , LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code ORDER BY tm_key_yr) AS prev_ga_ytd
--                 , m1_ytd
--                 , LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code ORDER BY tm_key_yr) AS prev_m1_ytd
--                 , CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
--                 , ga_target_ytd
--             FROM (
--                 SELECT tm_key_yr, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name
--                     , SUM(ga_ytd) AS ga_ytd
--                     , SUM(m1_ytd) AS m1_ytd
--                     , SUM(ga_target_ytd) AS ga_target_ytd
--                 FROM W_TXN_YTD_BY_PARTNER AS TOTAL_YTD
--                 GROUP BY tm_key_yr

--                 UNION ALL 

--                 SELECT * FROM W_TXN_YTD_BY_PARTNER	
--                 WHERE ga_ytd <> 0
--             ) T
--         ) T1
--     ) T2
--     WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)
-- ) T3

-- WHERE product = 'ALL' OR (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3)

-- ORDER BY tm_key_yr, product, ga_top_partner_rnk

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, partner, segment

-- SELECT tm_key_yr, product, partner_code, partner_name, group_sim
--     , ga_ytd, ga_yoy, ga_partner_ytd
--     , ga_top_partner_rnk, ga_bot_partner_rnk
-- FROM (
--     SELECT tm_key_yr, product, partner_code, partner_name, group_sim
--         , ga_ytd, ga_ach, ga_yoy, prev_ga_ytd, ga_partner_ytd
--         , CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY ga_partner_ytd DESC NULLS LAST, partner_name) END ga_top_partner_rnk
--         , CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY ga_partner_ytd NULLS LAST, partner_name) END ga_bot_partner_rnk
--         , m1_ytd, m1_yoy, prev_m1_ytd
--         , arpu_ytd
--         , CASE WHEN prev_arpu_ytd <> 0 THEN (arpu_ytd - prev_arpu_ytd) / prev_arpu_ytd * 100 END arpu_yoy
--         , prev_arpu_ytd
--     FROM (
--         SELECT tm_key_yr, product, partner_code, partner_name, group_sim
--             , ga_ytd, prev_ga_ytd
--             , CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
--             , ga_target_ytd
--             , CASE WHEN ga_target_ytd <> 0 THEN ga_ytd / ga_target_ytd * 100 END ga_ach
-- 			, ga_partner_ytd
--             , m1_ytd, prev_m1_ytd
--             , CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
--             , arpu_ytd
--             , LAG(arpu_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code, group_sim ORDER BY tm_key_yr) AS prev_arpu_ytd
--         FROM (
--             SELECT tm_key_yr, product, partner_code, partner_name, group_sim
--                 , ga_ytd
--                 , LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code, group_sim ORDER BY tm_key_yr) AS prev_ga_ytd
--                 , ga_target_ytd
-- 				, CASE WHEN product <> 'ALL' THEN SUM(ga_ytd) OVER (PARTITION BY tm_key_yr, product, partner_code) END ga_partner_ytd
--                 , m1_ytd
--                 , LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, partner_code, group_sim ORDER BY tm_key_yr) AS prev_m1_ytd
--                 , CASE WHEN COALESCE(ga_ytd,0) <> 0 THEN m1_ytd/ga_ytd END arpu_ytd
--             FROM (
--                 SELECT tm_key_yr, 'ALL' AS product, 'ALL' AS partner_code, 'ALL' AS partner_name, 'ALL' AS group_sim
--                     , SUM(ga_ytd) AS ga_ytd
--                     , SUM(m1_ytd) AS m1_ytd
--                     , SUM(ga_target_ytd) AS ga_target_ytd
--                 FROM W_TXN_YTD_BY_PARTNER_SKU AS TOTAL_YTD
--                 GROUP BY tm_key_yr

--                 UNION ALL 

--                 SELECT * FROM W_TXN_YTD_BY_PARTNER_SKU	
--                 WHERE ga_ytd <> 0
--             ) T
--         ) T1
--     ) T2
--     WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)
-- ) T3

-- WHERE product = 'ALL' OR (ga_top_partner_rnk <= 3 OR ga_bot_partner_rnk <= 3)

-- ORDER BY tm_key_yr, product, ga_top_partner_rnk, group_sim

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


--> YTD summary by product, channel, partner

-- SELECT tm_key_yr, product, channel, partner_code, partner_name
--     , ga_ytd--, ga_yoy--, prev_ga_ytd
--     , m1_ytd, m1_yoy--, prev_m1_ytd
-- 	, m1_channel_ytd, m1_channel_yoy
-- 	, ga_top_channel_rnk, ga_bot_channel_rnk

-- FROM (
--     SELECT tm_key_yr, product, channel, partner_code, partner_name
--         , ga_ytd, ga_yoy, prev_ga_ytd
--         , m1_ytd, m1_yoy, prev_m1_ytd
-- 		, m1_channel_ytd, m1_channel_yoy
--         , CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY m1_channel_ytd DESC NULLS LAST, m1_channel_yoy DESC NULLS LAST, channel) END ga_top_channel_rnk
-- 		, CASE WHEN product <> 'ALL' THEN DENSE_RANK() OVER (PARTITION BY tm_key_yr, product ORDER BY m1_channel_ytd NULLS LAST, m1_channel_yoy NULLS LAST, channel DESC) END ga_bot_channel_rnk
--     FROM (
--         SELECT tm_key_yr, product, channel, partner_code, partner_name
--             , ga_ytd, prev_ga_ytd
--             , CASE WHEN prev_ga_ytd <> 0 THEN (ga_ytd - prev_ga_ytd) / prev_ga_ytd * 100 END ga_yoy
--             , m1_ytd, prev_m1_ytd
--             , CASE WHEN prev_m1_ytd <> 0 THEN (m1_ytd - prev_m1_ytd) / prev_m1_ytd * 100 END m1_yoy
-- 			, m1_channel_ytd, m1_channel_yoy
--         FROM (
--             SELECT tm_key_yr, product, channel, partner_code, partner_name
--                 , ga_ytd
--                 , LAG(ga_ytd IGNORE NULLS) OVER (PARTITION BY product, channel, partner_code ORDER BY tm_key_yr) AS prev_ga_ytd
--                 , m1_ytd
--                 , LAG(m1_ytd IGNORE NULLS) OVER (PARTITION BY product, channel, partner_code ORDER BY tm_key_yr) AS prev_m1_ytd
-- 				, m1_channel_ytd, m1_channel_yoy
--             FROM (
--                 SELECT tm_key_yr, 'ALL' AS product, 'ALL' AS channel, 'ALL' AS partner_code, 'ALL' AS partner_name
--                     , SUM(ga_ytd) AS ga_ytd
--                     , SUM(m1_ytd) AS m1_ytd
-- 					, NULL AS m1_channel_ytd
-- 					, NULL AS m1_channel_yoy
--                 FROM W_TXN_YTD_BY_CHANNEL_PARTNER AS TOTAL_YTD
--                 GROUP BY tm_key_yr

--                 UNION ALL 

--                 SELECT A.tm_key_yr, A.product, A.channel, A.partner_code, A.partner_name
-- 					, A.ga_ytd, A.m1_ytd
-- 					, B.m1_ytd AS m1_channel_ytd
-- 					, B.m1_yoy AS m1_channel_yoy
-- 				FROM W_TXN_YTD_BY_CHANNEL_PARTNER A
-- 				LEFT JOIN W_TXN_YTD_BY_CHANNEL B 
-- 					ON B.tm_key_yr = A.tm_key_yr
-- 					AND B.product = A.product
-- 					AND B.channel = A.channel
--             ) T
--         ) T1
--     ) T2
--     WHERE tm_key_yr = (SELECT curr_yr FROM W_PARAM)
-- 	AND (product = 'ALL' OR (m1_channel_ytd > 0 AND m1_channel_yoy < 0))
-- ) T3

-- WHERE product = 'ALL' 
-- OR ((ga_top_channel_rnk <= 3 OR ga_bot_channel_rnk <= 3)
-- 	AND m1_yoy > 0)

-- ORDER BY tm_key_yr, product, ga_top_channel_rnk