
SELECT TM_KEY_DAY, TM_KEY_WK, TM_KEY_MTH, TM_KEY_QTR, TM_KEY_YR, CENTER, PRODUCT_GRP, COMP_CD, METRIC_GRP, METRIC_NAME_GROUP, METRIC_CD, METRIC_NAME, AREA_TYPE, AREA_CD, AREA_NAME
    , ACTUAL_AS_OF, AGG_TYPE, RR_IND, GRY_IND, UOM, PERIOD, ACTUAL_SNAP, ACTUAL_AGG, TARGET_SNAP, TARGET_AGG, BASELINE_SNAP, BASELINE_AGG, ACH_SNAP, ACH_AGG, GAP_SNAP, GAP_AGG
    , WOW, WOW_PERCENT, MOM, MOM_PERCENT, QOQ, QOQ_PERCENT, YOY, YOY_PERCENT, RR, RR_ACH, WTD, MTD, QTD, YTD, PPN_TM
FROM GEOSPCAPPO.AGG_PERF_NEWCO
WHERE CENTER IN ('Revenue', 'Sales')
AND AREA_TYPE = 'P';