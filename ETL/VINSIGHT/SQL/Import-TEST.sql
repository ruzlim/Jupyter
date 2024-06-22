
/*** Broadband Marketshare to "AUTOKPI.FCT_BROADBAND_MKS" 

	-->> % MKS

		VIN00019	Broadband Subs Share : AIS & 3BB
		VIN00020	Broadband Subs Share : TOL
		VIN00021	Broadband Subs Share : 3BB
		VIN00022	Broadband Subs Share : AIS
		VIN00023	Broadband Subs Share : NT
		
	-->> Subs

		VIN00024	Broadband Subs Share (Subs) : AIS & 3BB
		VIN00025	Broadband Subs Share (Subs) : TOL
		VIN00026	Broadband Subs Share (Subs) : 3BB
		VIN00027	Broadband Subs Share (Subs) : AIS
		VIN00028	Broadband Subs Share (Subs) : NT
***/
-----------------------------------------------------------------------------------------------------------------------

WITH W_PARAM AS
(
	SELECT :mth_start AS V_ACTUAL_MONTH_START
		, :mth_end AS V_ACTUAL_MONTH_END
		, TO_NUMBER(TO_CHAR(CURRENT_DATE + 7, 'YYYYMM')) AS V_PREP_MONTH
	FROM DUAL
) -->> W_PARAM

SELECT * FROM W_PARAM
;