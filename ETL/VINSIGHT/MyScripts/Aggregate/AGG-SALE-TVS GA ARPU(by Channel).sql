
/***
 	Aggregate : TVS GA ARPU by channel 12 KPIs (All area)
	
	TRUE : 12 KPIs(All Channel)
		TB4R001600	TVS GA ARPU = ("TB4R001000 TVS Inflow M1" / "TB4S000100 TVS Gross Adds")
***/
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


/*** Temp Parameter ***/

WITH W_PARAM AS
(
	SELECT 2024 AS TM_KEY_YR FROM DUAL
)
-->> fetched - about 1-3 min.
-----------------------------------------------------------------------------------------------------------------------


/** Aggregate Sctips ***/


SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, METRIC_VALUE, COMP_CD, VERSION, AREA_CD, AREA_NAME, AREA_TYPE
	, CURRENT_DATE AS LOAD_DATE, REMARK
	
FROM ( 

	-->> TB4R001600 TVS GA ARPU = ("TB4R001000 TVS Inflow M1" / "TB4S000100 TVS Gross Adds")
	SELECT M1_T.TM_KEY_DAY
		, REPLACE(M1_T.METRIC_CD, 'TB4R001000', 'TB4R001600') AS METRIC_CD
		, REPLACE(M1_T.METRIC_NAME, 'Inflow M1', 'GA ARPU') AS METRIC_NAME
		, CASE WHEN COALESCE(GA_T.ACTUAL_AGG_MTH,0) <> 0 THEN (M1_T.ACTUAL_AGG_MTH / GA_T.ACTUAL_AGG_MTH) END METRIC_VALUE -->> Calculation
		, 'TRUE' AS COMP_CD, 'A' AS VERSION, M1_T.AREA_CD, M1_T.AREA_NAME, M1_T.AREA_TYPE
		, M1_T.METRIC_CD||' '||M1_T.METRIC_NAME||' / '||GA_T.METRIC_CD||' '||GA_T.METRIC_NAME AS REMARK
		
	FROM ( -->> TB4R001000xx TB4R001000 TVS Inflow M1 (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO 
		WHERE (METRIC_CD = 'TB4R001000' OR REGEXP_LIKE(METRIC_CD, '^TB4R001000A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) M1_T
	
	LEFT JOIN ( -->> TB4S000100xx TB4S000100 TVS Gross Adds (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_CD, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO 
		WHERE (METRIC_CD = 'TB4S000100' OR REGEXP_LIKE(METRIC_CD, '^TB4S000100A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) GA_T
		ON M1_T.CHANNEL_CD = GA_T.CHANNEL_CD
		AND M1_T.AREA_CD = GA_T.AREA_CD
		AND M1_T.TM_KEY_DAY = GA_T.TM_KEY_DAY
		
) TVS_GA_ARPU_BY_CHANNEL