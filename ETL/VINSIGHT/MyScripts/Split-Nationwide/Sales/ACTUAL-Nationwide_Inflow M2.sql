
/***
 	Actual : Sales Inflow M2 2 KPIs (for split Corporate, Nationwide)
 	
 	DTAC :
		DB1R001000	Prepaid Inflow M2 : DTAC ("P" only)
		
 	TRUE :
 		TB1R001000	Prepaid Inflow M2 : TMH ("P" only)
***/

-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


/*** Temp Parameter ***/

WITH W_PARAM AS
(
	SELECT 20240101 AS V_DAY_START FROM DUAL
)
-->> fetched - about 15-20 sec.
-----------------------------------------------------------------------------------------------------------------------


/** Aggregate Sctips ***/


SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME
	, ACTUAL_NUMERATOR - COALESCE(ACTUAL_DENOMINATOR,0) AS ACTUAL -->> ACTUAL Calculation
	, COMP_CD, VERSION, AREA_CD, AREA_NAME, AREA_TYPE
	, CURRENT_DATE AS LOAD_DATE, REMARK
	
FROM ( 
	
	-->> DB1R001000	Prepaid Inflow M2 : DTAC ("P" only)
	SELECT TM_KEY_DAY, 'DB1R001000' AS METRIC_CD, 'Prepaid Inflow M2 : DTAC' AS METRIC_NAME
		, COMP_CD, VERSION, 'P' AS AREA_TYPE, 'P' AS AREA_CD, 'Nationwide' AS AREA_NAME
		, SUM(CASE WHEN METRIC_CD = 'DB1R001000' THEN ACTUAL END) ACTUAL_NUMERATOR
		, SUM(CASE WHEN METRIC_CD IN ('DB1R001000AB', 'DB1R001000AD', 'DB1R001000AH', 'DB1R001000AI') THEN ACTUAL END) ACTUAL_DENOMINATOR
		, '"DB1R001000 Prepaid Inflow M2 : DTAC" (Exclude B2B, Contact Center, OTHERS, Own Digital)' AS REMARK
	FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL 
	WHERE METRIC_CD IN ('DB1R001000', 'DB1R001000AB', 'DB1R001000AD', 'DB1R001000AH', 'DB1R001000AI')
	AND TM_KEY_DAY >= (SELECT V_DAY_START FROM W_PARAM)
	AND AREA_TYPE = 'G' -->> Sum to Nationwide(Geo)
	AND NOT (AREA_CD IS NULL OR AREA_CD IN ('-99', 'Z00', '9GZ', 'Unidentified', 'True Corp'))
	GROUP BY TM_KEY_DAY, COMP_CD, VERSION
	
	UNION ALL 
	
	-->> TB1R001000	Prepaid Inflow M2 : TMH ("P" only)
	SELECT TM_KEY_DAY, 'TB1R001000' AS METRIC_CD, 'Prepaid Inflow M2 : TMH' AS METRIC_NAME
		, COMP_CD, VERSION, 'P' AS AREA_TYPE, 'P' AS AREA_CD, 'Nationwide' AS AREA_NAME
		, SUM(CASE WHEN METRIC_CD = 'TB1R001000' THEN ACTUAL END) ACTUAL_NUMERATOR
		, SUM(CASE WHEN METRIC_CD IN ('TB1R001000AB', 'TB1R001000AD', 'TB1R001000AH', 'TB1R001000AI') THEN ACTUAL END) ACTUAL_DENOMINATOR
		, '"TB1R001000 Prepaid Inflow M2 : TMH" (Exclude B2B, Contact Center, OTHERS, Own Digital)' AS REMARK
	FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL 
	WHERE METRIC_CD IN ('TB1R001000', 'TB1R001000AB', 'TB1R001000AD', 'TB1R001000AH', 'TB1R001000AI')
	AND TM_KEY_DAY >= (SELECT V_DAY_START FROM W_PARAM)
	AND AREA_TYPE = 'G' -->> Sum to Nationwide(Geo)
	AND NOT (AREA_CD IS NULL OR AREA_CD IN ('-99', 'Z00', '9GZ', 'Unidentified', 'True Corp'))
	GROUP BY TM_KEY_DAY, COMP_CD, VERSION
	
) ACTUAL_NATIONWIDE_INFLOW_M2
