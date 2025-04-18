
/*** Broadband Marketshare to "AUTOKPI.FCT_BROADBAND_MKS" (version Y2025) 

-->> % BB MKS

	VIN00080	Broadband Subs Share : AIS & 3BB (New)
	VIN00081	Broadband Subs Share : TOL (New)
	VIN00082	Broadband Subs Share : 3BB (New)
	VIN00083	Broadband Subs Share : AIS (New)
	VIN00084	Broadband Subs Share : NT (New)

-->> % BB Subs

	VIN00085	Broadband Subs Share (Subs) : AIS & 3BB
	VIN00086	Broadband Subs Share (Subs) : TOL
	VIN00087	Broadband Subs Share (Subs) : 3BB
	VIN00088	Broadband Subs Share (Subs) : AIS
	VIN00089	Broadband Subs Share (Subs) : NT
***/
-----------------------------------------------------------------------------------------------------------------------

WITH W_PARAM AS
(
	SELECT :mth_end_fct AS V_MTH_END_FCT
		, TO_NUMBER(TO_CHAR(CURRENT_DATE + 7, 'YYYYMM')) AS V_PREP_MTH_NEXT_WK
	FROM DUAL
) -->> W_PARAM
-----------------------------------------------------------------------------------------------------------------------

, W_PREP_PERIOD AS 
(
	SELECT TM_KEY_YR, TM_KEY_MTH, TRUE_TM_KEY_WK, TM_KEY_DAY
	FROM CDSAPPO.DIM_TIME
	WHERE TM_KEY_MTH > (SELECT V_MTH_END_FCT FROM W_PARAM)
	AND TM_KEY_MTH <= (SELECT V_PREP_MTH_NEXT_WK FROM W_PARAM)
) -->> W_PREP_PERIOD
-----------------------------------------------------------------------------------------------------------------------

, W_ORG AS 
( --7,436 row
	SELECT DISTINCT ZONE_TYPE
		, CASE WHEN (ORGID_G = 'GX3' AND HOP_HINT NOT LIKE 'SMP%') THEN 'Y' END EAST_FLAG
		, ORGID_G, TDS_SGMD--, ORGID_R, TDS_RGM_CODE
		, ORGID_H, HOP_HINT--, TDS_PROVINCE
		, SUBSTR(CCAATT,1,2) AS PROVINCE_CD, PROVINCE_ENG--, PROVINCE_TH
		, ORGID_HH, D_CLUSTER
		, SUBSTR(CCAATT,1,4) AS CCAA, DISTRICT_EN
		, CASE 	WHEN COUNT(DISTINCT SUBSTR(CCAATT,1,4)) OVER(PARTITION BY DISTRICT_EN) > 1
				THEN PROVINCE_ENG||'_'||DISTRICT_EN
				ELSE DISTRICT_EN
				END DISTRICT_UNIQUE
	FROM CDSAPPO.DIM_MOOC_AREA
	WHERE TEAM_CODE <> 'ไม่ระบุ' AND REMARK <> 'Dummy'
) -->> W_ORG
-----------------------------------------------------------------------------------------------------------------------

, W_ACTUAL_DIMENSION AS 
(
	SELECT P.TM_KEY_MTH
		, ZONE_TYPE, EAST_FLAG, ORGID_G, TDS_SGMD, ORGID_H, HOP_HINT, PROVINCE_CD, PROVINCE_ENG, ORGID_HH, D_CLUSTER, CCAA, DISTRICT_EN, DISTRICT_UNIQUE
	FROM (
		SELECT DISTINCT TM_KEY_MTH 
		FROM CORPNSBOX.FCT_BB_SHAREV4_ISP_SUBS
		WHERE VER = 'FINAL'
		AND AREA_TYPE IN ('TH', 'CCAA')
		AND TM_KEY_MTH > (SELECT V_MTH_END_FCT FROM W_PARAM)
	) P
	INNER JOIN W_ORG O
		ON 1=1
) -->> W_ACTUAL_DIMENSION
-----------------------------------------------------------------------------------------------------------------------

, W_BB_MKS_CORP AS
(
	SELECT TM_KEY_MTH, AREA_TYPE, AREA_CD 
		, SUM(COALESCE(TOL,0)) TOL, SUM(COALESCE(AIS,0)) AIS, SUM(COALESCE(TOT,0)) TOT, SUM(COALESCE(BBB,0)) BBB, SUM(COALESCE(CAT,0)) CAT, SUM(COALESCE(AISBBB,0)) AISBBB, SUM(COALESCE(NT,0)) NT
		, SUM(COALESCE(AISBBB,0) + COALESCE(TOL,0) + COALESCE(NT,0)) AS TOTAL
	FROM CORPNSBOX.FCT_BB_SHAREV4_ISP_SUBS
	WHERE VER = 'FINAL' 
	AND AREA_TYPE = 'TH'
	AND TM_KEY_MTH > (SELECT V_MTH_END_FCT FROM W_PARAM)
	GROUP BY TM_KEY_MTH, AREA_TYPE, AREA_CD
) -->> W_BB_MKS_CORP
-----------------------------------------------------------------------------------------------------------------------

, W_BB_MKS_CCAA AS 
(
	SELECT *
	FROM (
		SELECT TM_KEY_MTH, ZONE_TYPE, EAST_FLAG, ORGID_G, TDS_SGMD, ORGID_H, HOP_HINT, PROVINCE_CD, PROVINCE_ENG, ORGID_HH, D_CLUSTER, CCAA, DISTRICT_UNIQUE
			, SUM(COALESCE(TOL,0)) TOL, SUM(COALESCE(AIS,0)) AIS, SUM(COALESCE(TOT,0)) TOT, SUM(COALESCE(BBB,0)) BBB, SUM(COALESCE(CAT,0)) CAT, SUM(COALESCE(AISBBB,0)) AISBBB, SUM(COALESCE(NT,0)) NT
			, SUM(COALESCE(AISBBB,0) + COALESCE(TOL,0) + COALESCE(NT,0)) AS TOTAL
		FROM (
			SELECT D.TM_KEY_MTH, ZONE_TYPE, EAST_FLAG, ORGID_G, TDS_SGMD, ORGID_H, HOP_HINT, PROVINCE_CD, PROVINCE_ENG, ORGID_HH, D_CLUSTER, CCAA, DISTRICT_UNIQUE
				, A.TOL, A.AIS, A.TOT, A.BBB, A.CAT, A.AISBBB, A.NT
			FROM W_ACTUAL_DIMENSION D
			LEFT JOIN CORPNSBOX.FCT_BB_SHAREV4_ISP_SUBS A
				ON D.CCAA = A.AREA_CD
				AND A.VER = 'FINAL' 
				AND A.AREA_TYPE = 'CCAA'
				AND A.TM_KEY_MTH > (SELECT V_MTH_END_FCT FROM W_PARAM)
		) RAWDATA
		GROUP BY TM_KEY_MTH, ZONE_TYPE, EAST_FLAG, ORGID_G, TDS_SGMD, ORGID_H, HOP_HINT, PROVINCE_CD, PROVINCE_ENG, ORGID_HH, D_CLUSTER, CCAA, DISTRICT_UNIQUE
	) TMP
	WHERE TOTAL > 0
) -->> W_BB_MKS_CCAA
-----------------------------------------------------------------------------------------------------------------------

, W_BB_MKS_AGG AS 
(
	-->> C : Corporate
	SELECT TM_KEY_MTH, 0 AS AREA_NO, 'C' AS AREA_TYPE, 'C' AS AREA_CD, 'Corporate' AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CORP
	GROUP BY TM_KEY_MTH
	
	UNION ALL 
	
	-->> P : Nationwide
	SELECT TM_KEY_MTH, 1 AS AREA_NO, 'P' AS AREA_TYPE, 'P' AS AREA_CD, 'Nationwide' AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH
	
	UNION ALL 
	
	-->> ZONE : BMA & UPC
	SELECT TM_KEY_MTH, 2 AS AREA_NO, 'Z' AS AREA_TYPE, ZONE_TYPE AS AREA_CD, ZONE_TYPE AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, ZONE_TYPE
	
	UNION ALL 
	
	-->> ZONE : East (Included in the UPC)
	SELECT TM_KEY_MTH, 3 AS AREA_NO, 'Z' AS AREA_TYPE, 'EAST' AS AREA_CD, 'East' AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	WHERE EAST_FLAG = 'Y'
	GROUP BY TM_KEY_MTH, EAST_FLAG
	
	UNION ALL 
	
	-->> MAIN Province : 4 PROVINCE
	SELECT TM_KEY_MTH, 4 AS AREA_NO, 'Z' AS AREA_TYPE, PROVINCE_CD AS AREA_CD, PROVINCE_ENG AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	WHERE PROVINCE_CD IN (10, 11, 12, 13) -->> 'Bangkok, Samut Prakan, Nonthaburi, Pathum Thani'
	GROUP BY TM_KEY_MTH, PROVINCE_CD, PROVINCE_ENG
	
	UNION ALL 
	
	-->> G : 8 Regional
	SELECT TM_KEY_MTH, 5 AS AREA_NO, 'G' AS AREA_TYPE, ORGID_G AS AREA_CD, TDS_SGMD AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, ORGID_G, TDS_SGMD
	
	UNION ALL 
	
	-->> H : 65 HOP_HINT
	SELECT TM_KEY_MTH, 6 AS AREA_NO, 'H' AS AREA_TYPE, ORGID_H AS AREA_CD, HOP_HINT AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, ORGID_H, HOP_HINT
	
	UNION ALL 
	
	-->> PV : 77 Province
	SELECT TM_KEY_MTH, 7 AS AREA_NO, 'PV' AS AREA_TYPE, PROVINCE_CD AS AREA_CD, PROVINCE_ENG AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, PROVINCE_CD, PROVINCE_ENG
	
	UNION ALL 
	
	-->> HH : 96 D_CLUSTER
	SELECT TM_KEY_MTH, 8 AS AREA_NO, 'HH' AS AREA_TYPE, ORGID_HH AS AREA_CD, D_CLUSTER AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, ORGID_HH, D_CLUSTER
	
	UNION ALL 
	
	-->> CCAA : 928 DISTRICT_UNIQUE
	SELECT TM_KEY_MTH, 9 AS AREA_NO, 'CCAA' AS AREA_TYPE, CCAA AS AREA_CD, DISTRICT_UNIQUE AS AREA_NAME
		, SUM(AISBBB) AISBBB, SUM(TOL) TOL, SUM(BBB) BBB, SUM(AIS) AIS, SUM(NT) NT, SUM(TOTAL) TOTAL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AISBBB)/SUM(TOTAL)*100) END MKS_AISBBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(TOL)/SUM(TOTAL)*100) END MKS_TOL
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(BBB)/SUM(TOTAL)*100) END MKS_BBB
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(AIS)/SUM(TOTAL)*100) END MKS_AIS
		, CASE WHEN SUM(TOTAL) = 0 THEN 0 ELSE (SUM(NT)/SUM(TOTAL)*100) END MKS_NT
	FROM W_BB_MKS_CCAA
	GROUP BY TM_KEY_MTH, CCAA, DISTRICT_UNIQUE
) -->> W_BB_MKS_AGG
-----------------------------------------------------------------------------------------------------------------------

, W_RAW_BROADBAND_MKS_MONTHLY AS
(
-->> % Share

	-->> VIN00080	Broadband Subs Share : AIS & 3BB
	SELECT TM_KEY_MTH, 'AIS & 3BB' AS OPERATOR, '%' AS UOM, 'VIN00080' AS METRIC_CD, 'Broadband Subs Share : AIS & 3BB' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, MKS_AISBBB AS METRIC_VALUE
		, AISBBB AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00081	Broadband Subs Share : TOL
	SELECT TM_KEY_MTH, 'TOL' AS OPERATOR, '%' AS UOM, 'VIN00081' AS METRIC_CD, 'Broadband Subs Share : TOL' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, MKS_TOL AS METRIC_VALUE
		, TOL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00082	Broadband Subs Share : 3BB
	SELECT TM_KEY_MTH, '3BB' AS OPERATOR, '%' AS UOM, 'VIN00082' AS METRIC_CD, 'Broadband Subs Share : 3BB' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, MKS_BBB AS METRIC_VALUE
		, BBB AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00083	Broadband Subs Share : AIS
	SELECT TM_KEY_MTH, 'AIS' AS OPERATOR, '%' AS UOM, 'VIN00083' AS METRIC_CD, 'Broadband Subs Share : AIS' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, MKS_AIS AS METRIC_VALUE
		, AIS AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00084	Broadband Subs Share : NT
	SELECT TM_KEY_MTH, 'NT' AS OPERATOR, '%' AS UOM, 'VIN00084' AS METRIC_CD, 'Broadband Subs Share : NT' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, MKS_NT AS METRIC_VALUE
		, NT AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
-->> No. of Subscriber
	
	-->> VIN00085	Broadband Subs Share (Subs) : AIS & 3BB
	SELECT TM_KEY_MTH, 'AIS & 3BB' AS OPERATOR, 'subs' AS UOM, 'VIN00085' AS METRIC_CD, 'Broadband Subs Share (Subs) : AIS & 3BB' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, AISBBB AS METRIC_VALUE
		, NULL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00086	Broadband Subs Share (Subs) : TOL
	SELECT TM_KEY_MTH, 'TOL' AS OPERATOR, 'subs' AS UOM, 'VIN00086' AS METRIC_CD, 'Broadband Subs Share (Subs) : TOL' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, TOL AS METRIC_VALUE
		, NULL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00087	Broadband Subs Share (Subs) : 3BB
	SELECT TM_KEY_MTH, '3BB' AS OPERATOR, 'subs' AS UOM, 'VIN00087' AS METRIC_CD, 'Broadband Subs Share (Subs) : 3BB' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, BBB AS METRIC_VALUE
		, NULL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00088	Broadband Subs Share (Subs) : AIS
	SELECT TM_KEY_MTH, 'AIS' AS OPERATOR, 'subs' AS UOM, 'VIN00088' AS METRIC_CD, 'Broadband Subs Share (Subs) : AIS' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, AIS AS METRIC_VALUE
		, NULL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
	
	UNION ALL 
	
	-->> VIN00089	Broadband Subs Share (Subs) : NT
	SELECT TM_KEY_MTH, 'NT' AS OPERATOR, 'subs' AS UOM, 'VIN00089' AS METRIC_CD, 'Broadband Subs Share (Subs) : NT' AS METRIC_NAME
		, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME
		, NT AS METRIC_VALUE
		, NULL AS SUBS_VALUE
	FROM W_BB_MKS_AGG
) -->> W_RAW_BROADBAND_MKS_MONTHLY
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


-->> Output to "AUTOKPI.FCT_BROADBAND_MKS"

SELECT *
FROM (
	-->> Actual data
	SELECT P.TM_KEY_YR, P.TM_KEY_MTH, P.TRUE_TM_KEY_WK, P.TM_KEY_DAY
		, A.METRIC_CD, A.METRIC_NAME
		, 'TRUE' AS COMP_CD, 'A' AS VERSION
		, A.AREA_NO, A.AREA_TYPE, A.AREA_CD, A.AREA_NAME, A.METRIC_VALUE, A.SUBS_VALUE
		, 'N' AS AGG_TYPE, 'Monthly' AS FREQUENCY, NULL AS REMARK
	FROM W_RAW_BROADBAND_MKS_MONTHLY A
	LEFT JOIN CDSAPPO.DIM_TIME P
		ON P.TM_KEY_MTH = A.TM_KEY_MTH
	
	UNION ALL 
	
	-->> Mockup data
	SELECT P.TM_KEY_YR, P.TM_KEY_MTH, P.TRUE_TM_KEY_WK, P.TM_KEY_DAY
		, A.METRIC_CD, A.METRIC_NAME
		, 'TRUE' AS COMP_CD, 'A' AS VERSION
		, A.AREA_NO, A.AREA_TYPE, A.AREA_CD, A.AREA_NAME, A.METRIC_VALUE, A.SUBS_VALUE
		, 'N' AS AGG_TYPE, 'Monthly' AS FREQUENCY
		, 'Data as of : '||MAX(A.TM_KEY_MTH) OVER(PARTITION BY 1) AS REMARK
	FROM W_RAW_BROADBAND_MKS_MONTHLY A
	LEFT JOIN W_PREP_PERIOD P
		ON P.TM_KEY_MTH > A.TM_KEY_MTH
) FCT_BROADBAND_MKS

ORDER BY TM_KEY_DAY, METRIC_CD, AREA_NO, AREA_CD
;

-->> Test
-- WHERE METRIC_CD = 'VIN00081'
-- AND AREA_CD = 'P'