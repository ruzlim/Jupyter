/*** 
	Prepaid AP1D by SKU(MoM) 

		Test Case: 11430, 11414, 11389
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
			20260301::INTEGER AS p_start_date, 20260430::INTEGER AS p_end_date 
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
	AND tds_sgmd = 'North'
) --> W_ORG
-----------------------------------------------------------------------------------------------------------------------


, W_PREPAID AS 
(
	SELECT tm_key_mth, tm_key_day, day, days_in_month--, mom_day
		, CASE WHEN day <= mom_day THEN 'Y' END mom_flag
		, product, group_sim
		, ap1d
	FROM (
		SELECT tm_key_mth, tm_key_day
			, SUBSTRING(tm_key_day, 7, 2)::INT AS day
			, EXTRACT(day FROM LAST_DAY(TO_DATE(tm_key_day, 'YYYYMMDD')))::INT AS days_in_month
			, P.mom_day
			, product, group_sim--, gp_sku
            , SUM(COALESCE(ap1d_1_49,0) 
                + COALESCE(ap1d_50_99,0)
                + COALESCE(ap1d_100_119,0)
                + COALESCE(ap1d_120_149,0)
                + COALESCE(ap1d_150_199,0)
                + COALESCE(ap1d_200_249,0)
                + COALESCE(ap1d_250_299,0)
                + COALESCE(ap1d_300up,0)
                ) AS ap1d
		FROM RWZHDP_CENTRAL_DATA.SL_AGG_DASH_PREPAID_DAY A
		CROSS JOIN W_PARAM P 
		WHERE A.tm_key_day BETWEEN P.p_start_date AND P.p_end_date
		AND sub_product = 'PREPAY'
        -- AND group_sim = 'MIGRANT'
		AND EXISTS (SELECT 1 FROM W_ORG O WHERE O.ccaatt = A.partner_ccaatt)
		GROUP BY tm_key_mth, tm_key_day, P.MOM_DAY, product, group_sim
	) TMP
) --> W_PREPAID

-- SELECT * FROM W_PREPAID
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MOM AS 
(
	SELECT tm_key_mth, product, group_sim
		, ap1d_mtd, prev_ap1d_mtd
		, CASE 	WHEN prev_ap1d_mtd <> 0 
				THEN (ap1d_mtd - prev_ap1d_mtd) / prev_ap1d_mtd * 100 
				END ap1d_mom
	FROM (
		SELECT tm_key_mth, product, group_sim
			, ap1d_mtd
			, LAG(ap1d_mtd) OVER (PARTITION BY group_sim ORDER BY tm_key_mth) AS prev_ap1d_mtd
		FROM (
			SELECT tm_key_mth, product, group_sim
				, SUM(ap1d) AS ap1d_mtd
			FROM W_PREPAID
			WHERE mom_flag = 'Y'
			GROUP BY tm_key_mth, product, group_sim
		) T1
	) T2
	WHERE tm_key_mth = (SELECT curr_tm_key_mth FROM W_PARAM)
) --> W_TXN_MOM

-- SELECT * FROM W_TXN_MOM
-----------------------------------------------------------------------------------------------------------------------


, W_TXN_MTD AS 
(
	SELECT A.tm_key_mth, A.product, A.group_sim
		, A.ap1d_mtd, B.ap1d_mom, B.ap1d_mtd AS ap1d_mtd_cal, B.prev_ap1d_mtd
	FROM (
        SELECT tm_key_mth, product, group_sim
            , SUM(ap1d) AS ap1d_mtd
        FROM W_PREPAID
        GROUP BY tm_key_mth, product, group_sim
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
		-- AP1D
		, ap1d_mtd
		, CASE WHEN prev_ap1d_mtd <> 0 THEN (ap1d_mtd_cal - prev_ap1d_mtd) / prev_ap1d_mtd * 100 END ap1d_mom
		, ap1d_mtd_cal, prev_ap1d_mtd
	FROM (
		SELECT tm_key_mth
			, SUM(ap1d_mtd) AS ap1d_mtd
			, SUM(ap1d_mtd_cal) AS ap1d_mtd_cal
			, SUM(prev_ap1d_mtd) AS prev_ap1d_mtd
		FROM W_TXN_MTD
		GROUP BY tm_key_mth
	) TOTAL_MTD
	
	UNION ALL 
	
	SELECT * FROM W_TXN_MTD
) MTD_SUMMARY

ORDER BY tm_key_mth, product, group_sim