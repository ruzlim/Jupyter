

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
			20260802::INTEGER AS p_start_date, 20260802::INTEGER AS p_end_date 
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
	SELECT tm_key_mth, tm_key_day, product
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
-----------------------------------------------------------------------------------------------------------------------


, W_MOBILE_BY_COMP AS 
(
	SELECT tm_key_mth, tm_key_day, product, company, ga, m1
	FROM (
		SELECT * FROM W_PREPAID
		UNION ALL 
		SELECT * FROM W_POSTPAID
	) MB
) --> W_MOBILE_BY_COMP
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


SELECT tm_key_mth, tm_key_day, product, company, ga, m1
FROM W_MOBILE_BY_COMP
ORDER BY tm_key_day, product, company
