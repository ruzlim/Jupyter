
/*** Mockup last actual data for VINSIGHT display to "AUTOKPI.FCT_BROADBAND_MKS" ***/

-----------------------------------------------------------------------------------------------------------------------

--DELETE AUTOKPI.FCT_BROADBAND_MKS WHERE REMARK IS NOT NULL

INSERT INTO AUTOKPI.FCT_BROADBAND_MKS (TM_KEY_YR, TM_KEY_MTH, TRUE_TM_KEY_WK, TM_KEY_DAY, METRIC_CD, METRIC_NAME, COMP_CD, VERSION, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME, METRIC_VALUE, SUBS_VALUE, AGG_TYPE, FREQUENCY, REMARK)

-----------------------------------------------------------------------------------------------------------------

WITH W_PARAM AS
(
	SELECT :mth_end_fct AS V_MTH_END_FCT
		, :mth_end_src AS V_MTH_END_SRC
		, TO_NUMBER(TO_CHAR(CURRENT_DATE + 7, 'YYYYMM')) AS V_PREP_MTH_NEXT_WK
	FROM DUAL
) -->> W_PARAM
-----------------------------------------------------------------------------------------------------------------------

, W_PREP_PERIOD AS 
(
	SELECT TM_KEY_YR, TM_KEY_MTH, TRUE_TM_KEY_WK, TM_KEY_DAY
	FROM AUTOKPI.DIM_TIME 
	WHERE TM_KEY_MTH > (SELECT V_MTH_END_FCT FROM W_PARAM)
	AND TM_KEY_MTH <= (SELECT V_PREP_MTH_NEXT_WK FROM W_PARAM)
) -->> W_PREP_PERIOD
-----------------------------------------------------------------------------------------------------------------------

, W_RAW_BROADBAND_MKS_LAST_MTH_ACTUAL AS
(
	SELECT TM_KEY_MTH, METRIC_CD, METRIC_NAME, COMP_CD, VERSION, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME, METRIC_VALUE, SUBS_VALUE, AGG_TYPE, FREQUENCY
	FROM AUTOKPI.FCT_BROADBAND_MKS 
	WHERE TM_KEY_MTH = (SELECT V_MTH_END_FCT FROM W_PARAM)
	AND TM_KEY_DAY LIKE '%01'
) -->> W_RAW_BROADBAND_MKS_LAST_MTH_ACTUAL
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------


-->> Output to "FCT_BROADBAND_MKS"

SELECT P.TM_KEY_YR, P.TM_KEY_MTH, P.TRUE_TM_KEY_WK, P.TM_KEY_DAY
	, METRIC_CD, METRIC_NAME, COMP_CD, VERSION, AREA_NO, AREA_TYPE, AREA_CD, AREA_NAME, METRIC_VALUE, SUBS_VALUE, AGG_TYPE, FREQUENCY
	, 'Data as of : '||(SELECT V_MTH_END_SRC FROM W_PARAM) AS REMARK
FROM W_RAW_BROADBAND_MKS_LAST_MTH_ACTUAL A
LEFT JOIN W_PREP_PERIOD P
	ON 1=1
WHERE A.TM_KEY_MTH = (SELECT V_MTH_END_FCT FROM W_PARAM)
ORDER BY TM_KEY_DAY, METRIC_CD, AREA_NO, AREA_CD
;

-->> Test
-- AND METRIC_CD = 'VIN00081' AND AREA_CD = 'P'