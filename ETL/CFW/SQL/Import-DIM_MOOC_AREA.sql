
/*** Import : "CDSAPPO.DIM_MOOC_AREA" to "dim_mooc_area.csv" ***/

-----------------------------------------------------------------------------------------------------------------------

SELECT ZONE_TYPE, TEAM_CODE, SCAB_CODE, CCAATT, PROVINCE_KEY, PROVINCE_TH, DISTRICT_TH, SUB_DISTRICT_TH, PROVINCE_ENG, DISTRICT_EN, SUB_DISTRICT_EN, TDS_RGM, TDS_PROVINCE, TDS_SGMD, ORGID_P, ORG_LEVEL_P, ORGID_G, ORG_LEVEL_G, ORGID_R, ORG_LEVEL_R, ORGID_H, ORG_LEVEL_H, ORGID_AA, ORG_LEVEL_AA, ORG_LEVEL_AA_NAME, ORG_LEVEL_AA_DESC, ORG_ID_A, ORG_LEVEL_A, PPN_TM, UPD_TM, TDS_RGM_CODE, HOP_HINT, REMARK, SPECIAL_AA, ORG_GRAD, D_CLUSTER, D_HOP_HINT, HOP_HINT_TH, HOP_HINT_EN, TDS_GMD, ORGID_HH, ORG_LEVEL_HH
FROM CDSAPPO.DIM_MOOC_AREA NOLOCK
;