
/*** Import : "AGG_PERF_NEWCO" to "AGG_PERF_NEWCO_SNAP" ***/

-----------------------------------------------------------------------------------------------------------------------

WITH W_PARAM AS 
(
	SELECT :yr AS V_YR
		, :mth_start AS V_MTH_START
		, :mth_end AS V_MTH_END
		, :dt AS V_DT 
	FROM DUAL
)
-----------------------------------------------------------------------------------------------------------------------

SELECT TM_KEY_YR, TM_KEY_QTR, TM_KEY_MTH, TM_KEY_WK, TM_KEY_DAY
	, CENTER, PRODUCT_GRP, COMP_CD, METRIC_GRP, METRIC_CD, METRIC_NAME, SEQ
	, ACTUAL_AS_OF, AGG_TYPE, RR_IND, GRY_IND, UOM
	, AREA_TYPE, AREA_CD, AREA_NAME
	, ACTUAL_SNAP, TARGET_SNAP, BASELINE_SNAP, ACTUAL_AGG, TARGET_AGG, BASELINE_AGG
	, PPN_TM

FROM GEOSPCAPPO.AGG_PERF_NEWCO NOLOCK

WHERE CENTER IN ('Revenue', 'Sales')
AND NOT REGEXP_LIKE(METRIC_CD, '[0-9]C$|[0-9]H$|[0-9]MCOM$') --|[0-9]CORP$|[0-9]GEO$|[0-9]A[A-K]$
AND TM_KEY_MTH BETWEEN (SELECT V_MTH_START FROM W_PARAM) AND (SELECT V_MTH_END FROM W_PARAM)
-- AND TM_KEY_YR = (SELECT V_YR FROM W_PARAM)
-- AND TM_KEY_MTH = (SELECT V_MTH_END FROM W_PARAM)
-- AND TM_KEY_DAY = (SELECT V_DT FROM W_PARAM)
;