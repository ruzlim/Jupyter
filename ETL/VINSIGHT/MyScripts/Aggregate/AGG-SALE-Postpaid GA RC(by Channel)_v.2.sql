
/***
 	Aggregate : Postpaid GA RC by channel 39 KPIs (All area)
	
	ALL : 13 KPIs(All Channel)
		B2R000600	Postpaid GA RC = ("B2R000500 Postpaid Inflow M1" / "B2S000100 Postpaid Gross Adds")
		
	DTAC : 13 KPIs(All Channel)
		DB2R000600	Postpaid GA RC : DTAC = ("DB2R000500 Postpaid Inflow M1 : DTAC" / "DB2S000100 Postpaid Gross Adds : DTAC")
		
	TRUE : 13 KPIs(All Channel)
		TB2R000600	Postpaid GA RC : TMH = ("TB2R000500 Postpaid Inflow M1 : TMH" / "TB2S000100 Postpaid Gross Adds : TMH")
***/
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


/*** Temp Parameter ***/

WITH W_PARAM AS
(
	SELECT 2024 AS TM_KEY_YR FROM DUAL
)
-->> fetched : 1-3 min.
-----------------------------------------------------------------------------------------------------------------------


/** Aggregate Sctips ***/


SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, METRIC_VALUE, COMP_CD, VERSION, AREA_CD, AREA_NAME, AREA_TYPE
	, CURRENT_DATE AS LOAD_DATE, REMARK
	
FROM ( 

	-->> B2R000600 Postpaid GA RC = ("B2R000500 Postpaid Inflow M1" / "B2S000100 Postpaid Gross Adds")
	SELECT M1.TM_KEY_DAY
		, REPLACE(M1.METRIC_CD, 'B2R000500', 'B2R000600') AS METRIC_CD
		, REPLACE(M1.METRIC_NAME, 'Inflow M1', 'GA RC') AS METRIC_NAME
		, CASE WHEN COALESCE(GA.ACTUAL_AGG_MTH,0) <> 0 THEN (M1.ACTUAL_AGG_MTH / GA.ACTUAL_AGG_MTH) END METRIC_VALUE -->> Calculation
		, 'ALL' AS COMP_CD, 'A' AS VERSION, M1.AREA_CD, M1.AREA_NAME, M1.AREA_TYPE
		, M1.METRIC_CD||' '||M1.METRIC_NAME||' / '||GA.METRIC_CD||' '||GA.METRIC_NAME AS REMARK
		
	FROM ( -->> B2R000500xx Postpaid Inflow M1 (by Channel)
		SELECT TM_KEY_MTH, TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'B2R000500' OR REGEXP_LIKE(METRIC_CD, '^B2R000500A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) M1
	
	LEFT JOIN ( -->> B2S000100xx Postpaid Gross Adds (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_CD, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'B2S000100' OR REGEXP_LIKE(METRIC_CD, '^B2S000100A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) GA
		ON M1.CHANNEL_CD = GA.CHANNEL_CD
		AND M1.AREA_CD = GA.AREA_CD
		AND M1.TM_KEY_DAY = GA.TM_KEY_DAY
	-----------------------------------------------------------------------------------------------------------------------
	
	UNION ALL 
	
	-->> DB2R000600 Postpaid GA RC : DTAC = ("DB2R000500 Postpaid Inflow M1 : DTAC" / "DB2S000100 Postpaid Gross Adds : DTAC")
	SELECT M1_D.TM_KEY_DAY
		, REPLACE(M1_D.METRIC_CD, 'DB2R000500', 'DB2R000600') AS METRIC_CD
		, REPLACE(M1_D.METRIC_NAME, 'Inflow M1', 'GA RC') AS METRIC_NAME
		, CASE WHEN COALESCE(GA_D.ACTUAL_AGG_MTH,0) <> 0 THEN (M1_D.ACTUAL_AGG_MTH / GA_D.ACTUAL_AGG_MTH) END METRIC_VALUE -->> Calculation
		, 'DTAC' AS COMP_CD, 'A' AS VERSION, M1_D.AREA_CD, M1_D.AREA_NAME, M1_D.AREA_TYPE
		, M1_D.METRIC_CD||' '||M1_D.METRIC_NAME||' / '||GA_D.METRIC_CD||' '||GA_D.METRIC_NAME AS REMARK
		
	FROM ( -->> DB2R000500xx Postpaid Inflow M1 : DTAC (by Channel)
		SELECT TM_KEY_MTH, TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'DB2R000500' OR REGEXP_LIKE(METRIC_CD, '^DB2R000500A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) M1_D
	
	LEFT JOIN ( -->> DB2S000100xx Postpaid Gross Adds : DTAC (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_CD, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'DB2S000100' OR REGEXP_LIKE(METRIC_CD, '^DB2S000100A[A-K]$'))  
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) GA_D
		ON M1_D.CHANNEL_CD = GA_D.CHANNEL_CD
		AND M1_D.AREA_CD = GA_D.AREA_CD
		AND M1_D.TM_KEY_DAY = GA_D.TM_KEY_DAY
	-----------------------------------------------------------------------------------------------------------------------
	
	UNION ALL 
	
	-->> TB2R000600	Postpaid GA RC : TMH = ("TB2R000500 Postpaid Inflow M1 : TMH" / "TB2S000100 Postpaid Gross Adds : TMH")
	SELECT M1_T.TM_KEY_DAY
		, REPLACE(M1_T.METRIC_CD, 'TB2R000500', 'TB2R000600') AS METRIC_CD
		, REPLACE(M1_T.METRIC_NAME, 'Inflow M1', 'GA RC') AS METRIC_NAME
		, CASE WHEN COALESCE(GA_T.ACTUAL_AGG_MTH,0) <> 0 THEN (M1_T.ACTUAL_AGG_MTH / GA_T.ACTUAL_AGG_MTH) END METRIC_VALUE -->> Calculation
		, 'TRUE' AS COMP_CD, 'A' AS VERSION, M1_T.AREA_CD, M1_T.AREA_NAME, M1_T.AREA_TYPE
		, M1_T.METRIC_CD||' '||M1_T.METRIC_NAME||' / '||GA_T.METRIC_CD||' '||GA_T.METRIC_NAME AS REMARK
		
	FROM ( -->> TB2R000500xx Postpaid Inflow M1 : TMH (by Channel)
		SELECT TM_KEY_MTH, TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'TB2R000500' OR REGEXP_LIKE(METRIC_CD, '^TB2R000500A[A-K]$'))
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) M1_T
	
	LEFT JOIN ( -->> TB2S000100xx Postpaid Gross Adds : TMH (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_CD, ACTUAL_AGG_MTH
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK
		WHERE (METRIC_CD = 'TB2S000100' OR REGEXP_LIKE(METRIC_CD, '^TB2S000100A[A-K]$'))
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) GA_T
		ON M1_T.CHANNEL_CD = GA_T.CHANNEL_CD
		AND M1_T.AREA_CD = GA_T.AREA_CD
		AND M1_T.TM_KEY_DAY = GA_T.TM_KEY_DAY
		
) POSTPAID_GA_RC_BY_CHANNEL