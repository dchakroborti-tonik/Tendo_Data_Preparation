create or replace table worktable_data_analysis.tendo_user_app_package_20250915 as
with rn as 
(
SELECT a2.ee_customer_id, a1.id, a1.user_id, a1.reference_number, 
  COALESCE(
    -- Try to extract UUID-like pattern with length > 30
    REGEXP_EXTRACT(a1.reference_number, r'[:#]([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})[:#W]?'),
    -- Fallback: any alphanumeric+dash string > 30 chars between delimiters
    REGEXP_EXTRACT(a1.reference_number, r'[:#]([a-zA-Z0-9-]{31,})[:#W]?')
  ) as cleaned_rn, a1.deleted_at, a1.created_at, a1.updated_at, 
FROM prj-prod-dataplatform.tendopay_raw.credo_lab_reference_numbers a1
join worktable_data_analysis.tendo_scorecard_features_data_20250915 a2 on a2.ee_customer_id = a1.user_id
where date(a1.created_at) <= date(a2.ee_onboarding_date)
qualify row_number() over(partition by a1.user_id order by date(a1.created_at) desc) = 1
),
aca as 
(select deviceId, REGEXP_REPLACE(deviceId, r'^#\d+:', '') deviceId_cleaned, run_date,
A.package_name,A.first_install_time,A.last_update_time,A.version_name,A.flags 
from prj-prod-dataplatform.credolab_tendo_raw.android_credolab_Application,unnest(Application) A)
select rn.ee_customer_id, rn.id, rn.user_id, rn.cleaned_rn reference_number 
, rn.deleted_at
, rn.created_at 
, DATETIME(rn.created_at, "Asia/Manila") created_date_manila
, rn.updated_at
, aca.deviceId_cleaned deviceId
, aca.run_date
, aca.package_name
, aca.first_install_time
, aca.last_update_time
, aca.version_name
, aca.flags
from rn  
inner join aca on aca.deviceId_cleaned = rn.cleaned_rn
where date(rn.created_at) <= '2025-09-14';

