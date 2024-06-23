
/*** Testing ***/

WITH W_PARAM AS
(
	SELECT 202405 AS V_LAST_MTH_FCT
		, 202405 AS V_LAST_MTH_SRC
		, TO_NUMBER(TO_CHAR(CURRENT_DATE + 7, 'YYYYMM')) AS V_PREP_MTH_NEXT_WK
	FROM DUAL
) -->> W_PARAM
-----------------------------------------------------------------------------------------------------------------------

-- WITH W_PARAM AS
-- (
-- 	SELECT :last_mth_fct AS V_LAST_MTH_FCT
-- 		, :last_mth_src AS V_LAST_MTH_SRC
-- 		, TO_NUMBER(TO_CHAR(CURRENT_DATE + 7, 'YYYYMM')) AS V_PREP_MTH_NEXT_WK
-- 	FROM DUAL
-- ) -->> W_PARAM

-- SELECT * FROM W_PARAM
-----------------------------------------------------------------------------------------------------------------------

, W_PREP_PERIOD AS 
(
	SELECT TM_KEY_YR, TM_KEY_MTH, TRUE_TM_KEY_WK, TM_KEY_DAY
	FROM CDSAPPO.DIM_TIME NOLOCK
	WHERE TM_KEY_MTH > (SELECT V_LAST_MTH_FCT FROM W_PARAM)
	AND TM_KEY_MTH <= (SELECT V_PREP_MTH_NEXT_WK FROM W_PARAM)
) -->> W_PREP_PERIOD

SELECT DISTINCT TM_KEY_MTH
FROM W_PREP_PERIOD
ORDER BY 1
;