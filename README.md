# Omni_doc
# run to find 4-5 hour time zone 
-- Check what timezone ev_read_hist stores data in
SELECT 
    RH.read_strt_dttm,
    RH.read_strt_dttm AT TIME ZONE 'America/New_York' as eastern_time,
    CB.charge_box_id
FROM billing_fpl_fplnw_consolidated.ev_read_hist RH
JOIN billing_fpl_fplnw_consolidated.ev_site S 
    ON RH.site_pk = S.site_pk AND S.crnt_row_flag = TRUE
JOIN billing_fpl_fplnw_consolidated.ev_charge_box CB 
    ON RH.charge_box_id = CB.charge_box_id 
    AND CB.connector_id = RH.connector_id
WHERE S.site_id = '5730'  -- or whichever affected premise
    AND RH.read_strt_dttm >= '2025-01-01'::timestamp with time zone
    AND RH.read_strt_dttm < '2025-01-02'::timestamp with time zone
ORDER BY RH.read_strt_dttm
LIMIT 20;
