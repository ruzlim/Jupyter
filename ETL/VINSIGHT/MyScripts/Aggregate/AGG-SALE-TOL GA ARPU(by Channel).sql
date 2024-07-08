
/***
 	Aggregate : TOL GA ARPU by channel 12 KPIs (All area)
	
	TRUE : 12 KPIs(All Channel)
		TB3R000800	TOL GA ARPU = ("TB3R000600 TOL Inflow M1 - Connected" / "TB3S000100 TOL Gross Adds - Connected")
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

	-->> TB3R000800	TOL GA ARPU = ("TB3R000600 TOL Inflow M1 - Connected" / "TB3S000100 TOL Gross Adds - Connected")
	SELECT M1_T.TM_KEY_DAY
		, REPLACE(M1_T.METRIC_CD, 'TB3R000600', 'TB3R000800') AS METRIC_CD
		, REPLACE(M1_T.METRIC_NAME, 'Inflow M1', 'GA ARPU') AS METRIC_NAME
		, CASE WHEN COALESCE(GA_T.ACTUAL_AGG,0) <> 0 THEN (M1_T.ACTUAL_AGG / GA_T.ACTUAL_AGG) * 100 END METRIC_VALUE -->> Calculation
		, 'TRUE' AS COMP_CD, 'A' AS VERSION, M1_T.AREA_CD, M1_T.AREA_NAME, M1_T.AREA_TYPE
		, M1_T.METRIC_CD||' '||M1_T.METRIC_NAME||' / '||GA_T.METRIC_CD||' '||GA_T.METRIC_NAME AS REMARK
		
	FROM ( -->> TB3R000600xx TOL Inflow M1 - Connected (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME, ACTUAL_AGG
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO 
		WHERE (METRIC_CD = 'TB3R000600' OR REGEXP_LIKE(METRIC_CD, '^TB3R000600A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) M1_T
	
	LEFT JOIN ( -->> TB3S000100xx TOL Gross Adds - Connected (by Channel)
		SELECT TM_KEY_DAY, METRIC_CD, METRIC_NAME, AREA_CD, ACTUAL_AGG
			, CASE WHEN REGEXP_LIKE(SUBSTR(METRIC_CD,-2), 'A[A-K]$') THEN SUBSTR(METRIC_CD,-2) ELSE 'ALL' END CHANNEL_CD
		FROM GEOSPCAPPO.AGG_PERF_NEWCO 
		WHERE (METRIC_CD = 'TB3S000100' OR REGEXP_LIKE(METRIC_CD, '^TB3S000100A[A-K]$')) 
		AND TM_KEY_YR >= (SELECT TM_KEY_YR FROM W_PARAM)
	) GA_T
		ON M1_T.CHANNEL_CD = GA_T.CHANNEL_CD
		AND M1_T.AREA_CD = GA_T.AREA_CD
		AND M1_T.TM_KEY_DAY = GA_T.TM_KEY_DAY
		
) TOL_GA_ARPU_BY_CHANNEL
