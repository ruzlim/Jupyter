CREATE OR REPLACE PROCEDURE GEOSPCAPPO.PRC_STG_KPI_NEWCO_TOTAL (DATA_AS_OF_DAY  VARCHAR2) AS 

/* ================================================================= */
/* Created date   : 03-OCT-2022                                      */
/* Created by     : Krawin P.                                        */
/* Objective      : Support prc_stg_kpi_newco_total                  */
/* ================================================================= */
/* Modified date  : 08-OCT-2023                                       */
/* Modified by    : change source metric TB0R000100                  */
/* ================================================================= */
/* Modified date  :                                                  */
/* Modified by    :                                                  */
/* ================================================================= */
/* 2024-02-16 : fixed target not have only one source                */
/* 2024-02-20 : add total for B2R010100CUS                           */
/* 2024-02-28 : add total for B0R0001002                             */
/* 2024-03-05 : fixed actual not have only one source                */
/* 2024-03-11 : ALL : CORP B0R000100 = TB1R000100CORP + DB1R000100 + TB2R000100 + DB2R010100 + TB3R000100CORP + TB4R000100CORP + TNSC00147 */
/* 2024-03-12 : ALL : CORP B0R000100 = TB1R000100CORP + DB1R000100 + TB2R000100 + DB2R000100 + TB3R000100CORP + TB4R000100CORP + TNSC00147 */
/* 2024-03-13 : Fixed  B1R000100CORP and B2R000100 >>> same as_of_date data                                                                */
/* 2024-06-05 : New KPI B1S000500GEO                                                                                                       */
/* 2024-06-07 : New KPI B2R010503                                                                                                          */
/* 2024-06-10 : New KPI B1S000500GEO for G & H                                                                                             */
/* 2024-06-24 : Change Logic Churn                                                                                                         */
/* 2024-06-28 : New KPI B0R00010001GEO & B1R001000GEO                                                                                      */
/* 2024-07-10 : Re-Code                                                                                                                    */
/* 2024-07-16 : New KPI B0R00010002 , DB0R00010002 , TB0R00010002 , B0R00010001 , DB0R00010001 , TB0R00010001                              */
/* 2024-07-24 : New KPI B0R000100GEO , TB0R000100GEO , DB0R000100GEO                                                                       */
/* 2024-08-07 : New KPI B0R000100 , B0R000100GEO Fixed logic sum all                                                                       */
/* 2024-08-15 : New KPI B0R000101 , TB0R000101 , DB0R000101                                                                                */
/* 2024-10-24 : Modify logic KPI B0R00010002 , TB0R00010002 , B0R00010001 , TB0R00010001 by request K.Narut                                */
/* 2024-10-31 : Remove source area_type : CCAATT                                                                                           */
/* 2024-11-22 : Target Total All for : B2S010200 , B2S010201 , B2S010202                                                                   */
/* 2025-01-16 : Edit calulate : B2S010200                                                                                                  */
/* 2025-01-27 : Add Metric Total Inflow M1 : B0R00010001CS, B0R00010001CG, TB0R00010001CS, TB0R00010001CG, DB0R00010001CS, DB0R00010001CG  */

/* 2025-02-14 : Edit	-> 	Actual: B0R000100 Total Revenue : ALL = TB1R000100CORP + DB1R000100 + TB2R000100 + DB2R000100 + TB3R000100CORP + TB4R000100CORP
							Target: B0R000100 Total Revenue : ALL = TB1R000100 + DB1R000100 + TB2R000100 + DB2R000100 + TB3R000100 + TB4R000100
				Remove 	-> 	B2R010503 Postpaid Inflow M1 B2C (Actual & Target)
							B0R00010002 Total Gross Adds (Actual & Target)
	 						DB0R00010002 Total Gross Adds : DTAC (Actual & Target)
							TB0R00010002 Total Gross Adds : TRUE (Actual & Target)
							B0R00010001 Total Inflow M1 (Actual & Target)
							DB0R00010001 Total Inflow M1 : DTAC (Actual & Target)
							TB0R00010001 Total Inflow M1 : TRUE (Actual & Target) */


--V_SNAP_TM_KEY NUMBER := TO_NUMBER(DATA_AS_OF_DAY) ;
--V_TM_YEAR     NUMBER := TO_NUMBER(SUBSTR(TRIM(DATA_AS_OF_DAY),1,4)) ;
--V_TM_MONTH    NUMBER := TO_NUMBER(SUBSTR(TRIM(DATA_AS_OF_DAY),1,6)) ;
--V_TM_DATE     NUMBER := TO_NUMBER(SUBSTR(TRIM(DATA_AS_OF_DAY),7,2)) ;
V_SNAP_TM_KEY NUMBER := TO_NUMBER(20231224) ;

BEGIN

--ACTUAL
EXECUTE IMMEDIATE 'TRUNCATE TABLE GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL' ;
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000100'                           AS METRIC_CD
     , 'Total Revenue : TRUE'                 AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'TRUE'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100' , 'TB2R000100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000100'                           AS METRIC_CD
     , 'Total Revenue : DTAC'                 AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY 
AND METRIC_CD IN ( 'DB1R000100' , 'DB2R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R0001002'                           AS METRIC_CD
     , '%Revenue Growth'                      AS METRIC_NAME
     , ( ( SUM(ACTUAL) OVER (PARTITION BY METRIC_CD, COMP_CD, AREA_CD, SUBSTR(TM_KEY_DAY,1,4) ORDER BY TM_KEY_DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) - ACTUAL_YTD ) / ACTUAL_YTD ) * 100 AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC FCT
LEFT JOIN
        (
           SELECT SUBSTR(TM_KEY_DAY + 10000,1,6) AS TM_KEY_DAY_YTD
                , METRIC_CD                      AS METRIC_CD_YTD
                , COMP_CD                        AS COMP_CD_YTD
                , AREA_CD                        AS AREA_CD_YTD
                , SUM(ACTUAL) OVER (PARTITION BY METRIC_CD, COMP_CD, AREA_CD, SUBSTR(TM_KEY_DAY,1,4) ORDER BY TM_KEY_DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS ACTUAL_YTD
           FROM GEOSPCAPPO.FCT_KPI_NEWCO_NETWORK_ACTUAL_V2
           WHERE METRIC_CD = 'B0R000100MCOM'
		   AND AREA_TYPE <> 'CCAATT'
           AND ACTUAL IS NOT NULL
        ) YTD
        ON SUBSTR(TM_KEY_DAY,1,6) = YTD.TM_KEY_DAY_YTD
        AND METRIC_CD = METRIC_CD_YTD
        AND COMP_CD   = YTD.COMP_CD_YTD
        AND AREA_CD   = YTD.AREA_CD_YTD
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND AREA_TYPE <> 'CCAATT'
AND METRIC_CD = 'B0R000100MCOM'
;

COMMIT;
     
--ALL : CORP B0R000100 = TB1R000100 + DB1R000100 + TB2R000100 + DB2R000100 + TB3R000100 + TB4R000100
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000100'                            AS METRIC_CD
     , 'Total Revenue : ALL'                  AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100', 'DB1R000100' , 'TB2R000100' , 'DB2R000100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

--ALL : GEO B0R000100GEO = TB1R000100 + DB1R000100 + TB2R010100 + DB2R010100 + TB3R000100 + TB4R000100
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000100GEO'                         AS METRIC_CD
     , 'Total Revenue : ALL (GEO)'            AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100', 'DB1R000100' , 'TB2R010100' , 'DB2R010100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000200'                            AS METRIC_CD
     , 'Postpaid Churn Subs'                  AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000200' , 'TB2S000210' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary'        AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000201' , 'TB2S000211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary'      AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000202' , 'TB2S000212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010200'                            AS METRIC_CD
     , 'Postpaid Churn Subs B2C'              AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010200' , 'TB2S010200' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary B2C'    AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010201' , 'TB2S010211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary B2C'  AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010202' , 'TB2S010212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020200'                            AS METRIC_CD
     , 'Postpaid Churn Subs B2B'              AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020200' , 'TB2S020210' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary B2B'    AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020201' , 'TB2S020211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary B2B'  AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020202' , 'TB2S020212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB2S000201'                           AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary : DTAC' AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010201' , 'DB2S020201' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000100GEO'                        AS METRIC_CD
     , 'Total Revenue : TRUE (Geo)'           AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'TRUE'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100' , 'TB2R010100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000100GEO'                        AS METRIC_CD
     , 'Total Revenue : DTAC (Geo)'           AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB1R000100' , 'DB2R010100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000101'                            AS METRIC_CD
     , 'Mobile Revenue'                       AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2R000100' , 'TB2R000100' , 'DB1R000100' , 'TB1R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000101'                           AS METRIC_CD
     , 'Mobile Revenue : DTAC'                AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2R000100' , 'DB1R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000101'                           AS METRIC_CD
     , 'Mobile Revenue : TMH'                 AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'TRUE'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB2R000100' , 'TB1R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R00010001CS'                        AS METRIC_CD
     , 'Total Inflow M1'                      AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CS', 'TB1R000900CS', 'DB2R010500CS', 'DB2R020500CS', 'TB2R010500CS', 'TB2R020500CS', 'TB3R000601CS', 'TB3R000602CS', 'TB4R001004CS', 'TB4R001700CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R00010001CG'                        AS METRIC_CD
     , 'Total Inflow M1 - GEO Channel'        AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'ALL'                                  AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CG', 'TB1R000900CG', 'DB2R010500CG', 'DB2R020500CG', 'TB2R010500CG', 'TB2R020500CG', 'TB3R000601CG', 'TB3R000602CG', 'TB4R001004CG', 'TB4R001700CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R00010001CS'                       AS METRIC_CD
     , 'Total Inflow M1 : TRUE'               AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'TRUE'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('TB1R000900CS', 'TB2R010500CS', 'TB2R020500CS', 'TB3R000601CS', 'TB3R000602CS', 'TB4R001004CS', 'TB4R001700CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R00010001CG'                       AS METRIC_CD
     , 'Total Inflow M1 : TRUE - GEO Channel' AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'TRUE'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('TB1R000900CG', 'TB2R010500CG', 'TB2R020500CG', 'TB3R000601CG', 'TB3R000602CG', 'TB4R001004CG', 'TB4R001700CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R00010001CS'                       AS METRIC_CD
     , 'Total Inflow M1 : DTAC'               AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CS', 'DB2R010500CS', 'DB2R020500CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_ACTUAL
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R00010001CG'                       AS METRIC_CD
     , 'Total Inflow M1 : DTAC - GEO Channel' AS METRIC_NAME
     , SUM(ACTUAL)                            AS ACTUAL
     , 'DTAC'                                 AS COMP_CD
     , 'A'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_ACTUAL_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CG', 'DB2R010500CG', 'DB2R020500CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;





--TARGET
EXECUTE IMMEDIATE 'TRUNCATE TABLE GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET' ;
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000100'                           AS METRIC_CD
     , 'Total Revenue : TRUE'                 AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'TRUE'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100' , 'TB2R000100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;
     
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000100'                           AS METRIC_CD
     , 'Total Revenue : DTAC'                 AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'DTAC'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB1R000100' , 'DB2R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

--ALL : CORP B0R000100 = TB1R000100 + DB1R000100 + TB2R000100 + DB2R000100 + TB3R000100 + TB4R000100
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000100'                            AS METRIC_CD
     , 'Total Revenue : ALL'                  AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100', 'DB1R000100' , 'TB2R000100' , 'DB2R000100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

--ALL : GEO B0R000100GEO = TB1R000100 + DB1R000100 + TB2R010100 + DB2R010100 + TB3R000100 + TB4R000100
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000100GEO'                         AS METRIC_CD
     , 'Total Revenue : ALL (GEO)'            AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100', 'DB1R000100' , 'TB2R010100' , 'DB2R010100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000200'                            AS METRIC_CD
     , 'Postpaid Churn Subs'                  AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000200' , 'TB2S000210' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary'        AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000201' , 'TB2S000211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S000202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary'      AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S000202' , 'TB2S000212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010200'                            AS METRIC_CD
     , 'Postpaid Churn Subs B2C'              AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010200' , 'TB2S010200' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary B2C'    AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010201' , 'TB2S010211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary B2C'  AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010202' , 'TB2S010212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020200'                            AS METRIC_CD
     , 'Postpaid Churn Subs B2B'              AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020200' , 'TB2S020210' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary B2B'    AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020201' , 'TB2S020211' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET 
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S020202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary B2B'  AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S020202' , 'TB2S020212' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000100GEO'                        AS METRIC_CD
     , 'Total Revenue : TRUE (Geo)'           AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'TRUE'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB1R000100' , 'TB2R010100' , 'TB3R000100' , 'TB4R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000100GEO'                        AS METRIC_CD
     , 'Total Revenue : DTAC (Geo)'           AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'DTAC'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB1R000100' , 'DB2R010100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R000101'                            AS METRIC_CD
     , 'Mobile Revenue'                       AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2R000100' , 'TB2R000100' , 'DB1R000100' , 'TB1R000100')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R000101'                           AS METRIC_CD
     , 'Mobile Revenue : DTAC'                AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'DTAC'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2R000100' , 'DB1R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R000101'                           AS METRIC_CD
     , 'Mobile Revenue : TMH'                 AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'TRUE'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'TB2R000100' , 'TB1R000100' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;

COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010200'                            AS METRIC_CD
     , 'Postpaid Churn Subs B2C'              AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010200' , 'TB2S010200' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010201'                            AS METRIC_CD
     , 'Postpaid Churn Subs Voluntary B2C'    AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010201' , 'TB2S010201' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;
 
INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B2S010202'                            AS METRIC_CD
     , 'Postpaid Churn Subs Involuntary B2C'  AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ( 'DB2S010202' , 'TB2S010202' )
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R00010001CS'                        AS METRIC_CD
     , 'Total Inflow M1'                      AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CS', 'TB1R000900CS', 'DB2R010500CS', 'DB2R020500CS', 'TB2R010500CS', 'TB2R020500CS', 'TB3R000601CS', 'TB3R000602CS', 'TB4R001004CS', 'TB4R001700CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'B0R00010001CG'                        AS METRIC_CD
     , 'Total Inflow M1 - GEO Channel'        AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'ALL'                                  AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CG', 'TB1R000900CG', 'DB2R010500CG', 'DB2R020500CG', 'TB2R010500CG', 'TB2R020500CG', 'TB3R000601CG', 'TB3R000602CG', 'TB4R001004CG', 'TB4R001700CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R00010001CS'                       AS METRIC_CD
     , 'Total Inflow M1 : TRUE'               AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'TRUE'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('TB1R000900CS', 'TB2R010500CS', 'TB2R020500CS', 'TB3R000601CS', 'TB3R000602CS', 'TB4R001004CS', 'TB4R001700CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'TB0R00010001CG'                       AS METRIC_CD
     , 'Total Inflow M1 : TRUE - GEO Channel' AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'TRUE'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('TB1R000900CG', 'TB2R010500CG', 'TB2R020500CG', 'TB3R000601CG', 'TB3R000602CG', 'TB4R001004CG', 'TB4R001700CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R00010001CS'                       AS METRIC_CD
     , 'Total Inflow M1 : DTAC'               AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'DTAC'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CS', 'DB2R010500CS', 'DB2R020500CS')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;

INSERT /*+ APPEND */ INTO GEOSPCAPPO.STG_KPI_NEWCO_TOTAL_TARGET
SELECT /*+ parallel(8) */ TM_KEY_DAY          AS TM_KEY_DAY
     , 'DB0R00010001CG'                       AS METRIC_CD
     , 'Total Inflow M1 : DTAC - GEO Channel' AS METRIC_NAME
     , SUM(TARGET)                            AS TARGET
     , SUM(BASELINE)                          AS BASELINE
     , 'DTAC'                                 AS COMP_CD
     , 'T'                                    AS VERSION
     , AREA_CD                                AS AREA_CD
     , NULL                                   AS AREA_DESC
     , NULL                                   AS AREA_TYPE
     , SYSDATE                                AS LOAD_DATE
     , NULL                                   AS REMARK
     , 'Cal by Proc PRC_STG_KPI_NEWCO_TOTAL'  AS SRC
FROM GEOSPCAPPO.FCT_KPI_NEWCO_TARGET_INC FCT_TGT
WHERE TM_KEY_DAY >= V_SNAP_TM_KEY
AND METRIC_CD IN ('DB1R000900CG', 'DB2R010500CG', 'DB2R020500CG')
AND AREA_TYPE <> 'CCAATT'
GROUP BY TM_KEY_DAY, AREA_CD
;
 
COMMIT;





END PRC_STG_KPI_NEWCO_TOTAL ;
