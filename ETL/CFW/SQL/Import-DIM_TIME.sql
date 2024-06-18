
/*** Import : "CDSAPPO.DIM_TIME" to "dim_time.csv" ***/

-----------------------------------------------------------------------------------------------------------------------

WITH W_PARAM AS 
(
	SELECT :yr_start AS V_YR_START
		, :yr_end AS V_YR_END
	FROM DUAL
)
-----------------------------------------------------------------------------------------------------------------------

SELECT TM_KEY_DAY, DAY_DESC, DAY_SHORT, DAY_NO, DATE_VALUE, DAY_OF_WEEK, DAYS_IN_MONTH, TRUE_TM_KEY_WK, TRUE_WEEK, TM_KEY_WK, WEEK_YEAR, TM_KEY_MTH, MONTH_NO, MONTH_SHORT, TM_KEY_QTR, QUARTER_NO, TM_KEY_YR, YEAR_DESC, YEAR_DESC_TH
	, PERIODFLAG, ETL_DATE, ETL_UPDATE 
	, CURRENT_DATE AS LOAD_DATE
FROM CDSAPPO.DIM_TIME NOLOCK
WHERE TM_KEY_YR >= (SELECT V_YR_START FROM W_PARAM)
ORDER BY TM_KEY_DAY 
;