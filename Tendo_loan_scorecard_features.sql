with paid_payments as (   ----------------more payments belonging one repayment
select repayment_schedule_id,
sum(amount) as amount,
max(repaid_date) as repaid_date
from tendopay_raw.split_purchases
group by  repayment_schedule_id
),
delinquency as (
select u.id as user_id, u.product_type,
pr.id as loan_id,
pr.principal, pr.fee, pr.bir, pr.tendopay_installments as term,
substr(cast(date_trunc(pr.created_at, MONTH) as string),0,7) as mth,
rs.due_date,
RANK() over (partition by pr.id order by rs.due_date ) as installmentNumber,
rs.amount as annuity,
pp.amount as pp_amout, pp.repaid_date,
rs.outstanding_balance,
case when rs.amount - pp.amount > 0 then 1 else 0 end as flag_part_payment,
case
     when (repaid_date is not null and rs.amount <= pp.amount) then date_diff(pp.repaid_date, rs.due_date, day)
     when (repaid_date is not null and rs.amount > pp.amount) then date_diff(CURRENT_DATE, rs.due_date, day)
     when repaid_date is null and due_date < CURRENT_DATE then date_diff(CURRENT_DATE, rs.due_date, day)
end as DPD
from tendopay_raw.payment_responses pr
join tendopay_raw.users u on pr.tendopay_user_id=u.id
join tendopay_raw.repayment_schedules rs on rs.payment_response_id=pr.id
---all repayment schudeles joined
left join paid_payments pp on cast(rs.id as string)=pp.repayment_schedule_id
--all paid payment joined
where tendopay_disposition = 'success' and status='PTOK' --Status 'PTOK' is a successful loan, others are cancelled loans.
)
--------calculation of delinquencies
,min_inst_obs_def as (
select delinquency.user_id, delinquency.product_type, delinquency.loan_id,
min(DATE(due_date)) as min_loan_due_date,
max(installmentNumber) as max_installmentNumber, avg(principal) as principal, avg(annuity) as annuity,
  MIN(CASE WHEN installmentNumber = 1 THEN annuity END) AS first_outstanding_due_amount,
  MIN(CASE WHEN installmentNumber = 1 THEN outstanding_balance END) AS first_outstanding_balance,
  MIN(CASE WHEN installmentNumber = 2 THEN outstanding_balance END) AS second_outstanding_balance,
  MIN(CASE WHEN installmentNumber = 3 THEN outstanding_balance END) AS third_outstanding_balance,
  MIN(CASE WHEN installmentNumber = 4 THEN outstanding_balance END) AS fourth_outstanding_balance,
  MIN(CASE WHEN DPD >0 THEN installmentNumber  END  ) AS min_inst_def0,
  MIN(CASE WHEN DPD >=10 THEN installmentNumber  END ) AS min_inst_def10,
  MIN(CASE WHEN DPD >=30 THEN installmentNumber  END ) AS min_inst_def30,
  MIN(CASE WHEN DPD >=60 THEN installmentNumber   END ) AS min_inst_def60,
  MIN(CASE WHEN DPD >=90 THEN installmentNumber  END ) AS min_inst_def90,
  MIN(CASE WHEN DPD >=180 THEN installmentNumber END ) AS min_inst_def180,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) > 0 then installmentNumber end) as obs_min_inst_def0,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) >= 10 then installmentNumber end) as obs_min_inst_def10,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) >= 30 then installmentNumber end) as obs_min_inst_def30,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) >= 60 then installmentNumber end) as obs_min_inst_def60,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) >= 90 then installmentNumber end) as obs_min_inst_def90,
  max(case when DATE_DIFF(CURRENT_DATE,due_date, DAY) >= 180 then installmentNumber end) as obs_min_inst_def180
from delinquency
group by user_id, product_type, loan_id
),
fspd_data as (
SELECT
pr.id as loan_id,
min_loan_due_date,
sum(case when obs_min_inst_def30>=1 then 1 else 0 end) as obs_FPD30,
sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 then 1 else 0 end) as def_FPD30,
sum(case when obs_min_inst_def30>=2 then 1 else 0 end) as obs_SPD30,
sum(case when obs_min_inst_def30>=2 and min_inst_def30=2 then 1 else 0 end) as def_SPD30,
sum(case when obs_min_inst_def30>=3 then 1 else 0 end) as obs_TPD30,
sum(case when obs_min_inst_def30>=3 and min_inst_def30=3 then 1 else 0 end) as def_TPD30,
sum(case when obs_min_inst_def30>=1 then pr.principal else 0 end) as obs_FPD30_vol,
sum(case when obs_min_inst_def30>=4 then 1 else 0 end) as obs_FSTFPD30,
sum(case when obs_min_inst_def30>=4 and min_inst_def30=4 then 1 else 0 end) as def_FSTFPD30,
sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 and cast(pr.tendopay_installments as string) = '1' then first_outstanding_due_amount 
when obs_min_inst_def30>=1 and min_inst_def30=1 then first_outstanding_balance
else 0 end) as def_FPD30_vol,
sum(case when obs_min_inst_def30>=2 then pr.principal else 0 end) as obs_SPD30_vol,
sum(case when obs_min_inst_def30>=2 and min_inst_def30=2 then second_outstanding_balance else 0 end) as def_SPD30_vol,
sum(case when obs_min_inst_def30>=3 then pr.principal else 0 end) as obs_TPD30_vol,
sum(case when obs_min_inst_def30>=3 and min_inst_def30=3 then third_outstanding_balance else 0 end) as def_TPD30_vol,
sum(case when obs_min_inst_def30>=4 then pr.principal else 0 end) as obs_FSTFPD30_vol,
sum(case when obs_min_inst_def30>=4 and min_inst_def30=4 then fourth_outstanding_balance else 0 end) as def_FSTFPD30_vol,
FROM tendopay_raw.payment_responses pr
join tendopay_raw.users u on pr.tendopay_user_id=u.id
join min_inst_obs_def def on def.loan_id=pr.id
group by 1,2
),
cl_fspd_data as (
SELECT
u.id as user_id,
IF(DATE_DIFF(CURRENT_DATE(),MIN(DATE(pr.created_at)),DAY)>=60,1,0) AS cl_matured_fpd30_flag,
IF(DATE_DIFF(CURRENT_DATE(),MIN(DATE(pr.created_at)),DAY)>=90,1,0) AS cl_matured_fspd30_flag,
IF(DATE_DIFF(CURRENT_DATE(),MIN(DATE(pr.created_at)),DAY)>=120,1,0) AS cl_matured_fstpd30_flag,
IF(DATE_DIFF(CURRENT_DATE(),MIN(DATE(pr.created_at)),DAY)>=150,1,0) AS cl_matured_fstfpd30_flag,
CASE WHEN sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 then 1 else 0 end) >= 1 THEN 1 ELSE 0 END AS cl_fpd30_flag,

CASE WHEN sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 then 1 else 0 end) >= 1 OR sum(case when obs_min_inst_def30>=2 and min_inst_def30=2 then 1 else 0 end) >=1  THEN 1
ELSE 0
END cl_fspd30_flag,
CASE WHEN sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 then 1 else 0 end) >= 1 OR sum(case when obs_min_inst_def30>=2 and min_inst_def30=2 then 1 else 0 end) >=1 OR sum(case when obs_min_inst_def30>=3 and min_inst_def30=3 then 1 else 0 end) >= 1 THEN 1
ELSE 0
END cl_fstpd30_flag,
CASE WHEN sum(case when obs_min_inst_def30>=1 and min_inst_def30=1 then 1 else 0 end) >= 1 OR sum(case when obs_min_inst_def30>=2 and min_inst_def30=2 then 1 else 0 end) >=1 OR sum(case when obs_min_inst_def30>=3 and min_inst_def30=3 then 1 else 0 end) >= 1 OR sum(case when obs_min_inst_def30>=4 and min_inst_def30=4 then 1 else 0 end) >= 1 THEN 1
ELSE 0
END cl_fstfpd30_flag,
FROM tendopay_raw.payment_responses pr
join tendopay_raw.users u on pr.tendopay_user_id=u.id
join min_inst_obs_def def on def.loan_id=pr.id
group by 1
),
payment_channel_data as (
SELECT
pr.id AS loan_id,
STRING_AGG(DISTINCT vendor_id,'|')
from
tendopay_raw.customer_repayment_responses crs
JOIN tendopay_raw.split_purchases ON split_purchases.txnid = crs.txn_id
join tendopay_raw.repayment_schedules rs on cast(rs.id as string)=split_purchases.repayment_schedule_id
JOIN tendopay_raw.payment_responses pr on rs.payment_response_id=pr.id
GROUP BY 1
),
frozen_tags as (
SELECT user_id,
max(case when tag_id in (39, 100, 101, 102, 103) then 1 ELSE 0 END ) as frozen_tag
FROM `tendopay_raw.user_tag` 
group by user_id
),
data_preparation as (
SELECT u.id                                                                                    user_id,
       u.created_at                                                                            sign_up_date,
       ut.first_account_activated_at                                                           approval_date,
       u.employer_id                                                                           employer_id,
       e.name                                                                                  employer_name,
       ii.employment_date                                                                      employment_date,
       LENGTH(employment_date)                                                                 LENGTH_employment_date,
       datetime_diff(cast(ii.created_at as date), 
	   case               ------original: datetime_diff(cast(ut.first_account_activated_at as date),
        when LENGTH(employment_date)=6 then   date( cast(substr(employment_date, 1, 4) as int64), cast(substr(employment_date, 6, 1) as int64), 1)
        when LENGTH(employment_date)=7 then   date( cast(substr(employment_date, 1, 4) as int64), cast(substr(employment_date, 6, 2) as int64), 1)
        when LENGTH(employment_date)=10 then  date( cast(substr(employment_date, 1, 4) as int64), cast(substr(employment_date, 6, 2) as int64), cast(substr(employment_date, 9, 2) as int64))
      end, day) / 365 as employer_time,    ----sometimes employment_date is missing, sometimes approval_date (first_account_activated_at) is missing
       u.gender                                                                                gender,
       u.civil_status                                                                          civil_status,
       ii.employee_status                                                                      employment_status,
      case
        when ii.income in ('0-10000') then 5000
        when ii.income in ('10000-20000') then 15000
        when ii.income in ('20000-30000') then 25000
        when ii.income in ('30000-40000') then 35000
        when ii.income in ('40000-50000') then 45000
        when ii.income in ('50000+') then 50000
        else cast(ii.income as numeric)
      end as                                                                                   declared_income_num, 
       ii.verified_net_income                                                                  verified_net_income,
       case when tag.frozen_tag = 1 then 'Frozen' ELSE 'Not Frozen' END    Frozen_Status
       /*
       CASE
           WHEN   (SELECT COUNT(tag_id) FROM `tendopay_raw.user_tag` WHERE tag_id = 100 AND user_id = u.id) > 0
               OR (SELECT COUNT(tag_id) FROM `tendopay_raw.user_tag` WHERE tag_id = 101 AND user_id = u.id) > 0
               OR (SELECT COUNT(tag_id) FROM `tendopay_raw.user_tag` WHERE tag_id = 102 AND user_id = u.id) > 0
               OR (SELECT COUNT(tag_id) FROM `tendopay_raw.user_tag` WHERE tag_id = 103 AND user_id = u.id) > 0
               OR (SELECT COUNT(tag_id) FROM `tendopay_raw.user_tag` WHERE tag_id = 39 AND user_id = u.id) > 0
               THEN 'Frozen'
           ELSE 'Not Frozen' END                                                               Frozen_Status,
       --cci.value                                                                               Credit_limit,

        */
FROM `tendopay_raw.users` u
         LEFT JOIN `tendopay_raw.income_info` ii ON u.id = ii.user_id
         LEFT JOIN `tendopay_raw.user_timelines` ut ON u.id = ut.user_id
         LEFT JOIN `tendopay_raw.employers` e on u.employer_id = cast(e.id as string)
         LEFT JOIN frozen_tags tag on u.id = tag.user_id
         --LEFT JOIN customer_credit_information cci on u.id = cci.user_id
WHERE u.product_type in ('employer', 'payroll')
  --and u.account_activated = 2
  --and cci.`key` = 'credit-limit';
),
scoring_preparation as (
select  
  dp.*, 									
  case 
    when	employer_time		<	0.55	then	98			
    when	employer_time		<	2.35	then	123			
    when	employer_time		<	3.85	then	139			
    when	employer_time		>=	3.85	then	183			
    when	employer_time		is null		then	119			
    else					119	
  end as 	employer_time_score,	
  case 
    when	gender		in (	'Female'	 ) then	120			
    when	gender		in (	'not specified', 'Male'	 ) then	118			
    when	gender		is null		then	119			
    else					119	
  end as 	gender_score,	
  case 
    when	declared_income_num		<	21200	then	119			
    when	declared_income_num		<	26525	then	110			
    when	declared_income_num		<	33050	then	118			
    when	declared_income_num		>=	33050	then	146			
    when	declared_income_num		<	0	then	119			
    when	declared_income_num	is null		then	119			
    else			119	
  end as 	declared_income_score,	
  case 
    when	civil_status		is null		then	127			
    when	civil_status		in (	'Divorced','Married','Widowed'	 ) then	127			
    when	civil_status		in (	'Single'	 ) then	117			
    else		119	
  end as 	civil_status_score
from data_preparation dp
),scoring as (
select  
  sp.*, 
  employer_time_score+gender_score+declared_income_score+civil_status_score as score
from scoring_preparation sp
),
rating as (
select 
  user_id,
  frozen_status,
  employer_time_score,gender_score,declared_income_score,civil_status_score,
  case 
    when score > 532 then 'A'
    when score > 492 then 'B'
    when score > 478 then 'C'
    when score > 468 then 'D'
    when score > 452 then 'E'
    when score<= 452 then 'F'
    else null
  end as credit_rating  
from scoring sco
)
SELECT
--Employees related data
users.id as ee_customer_id,
users.email as ee_email,
users.phone as ee_phone_number,
users.firstname as ee_firstname,
users.middlename as ee_middlename,
users.lastname as ee_lastname,
users.birthdate ee_birthdate,
users.gender ee_gender,
users.email_verified as ee_email_verified_flag,
users.telephone_verified as ee_telephone_verified_flag,
users.id_verified as ee_id_verified_flag,
users.income_verified as ee_income_verified_flag,
users.morning_time as ee_morning_time_contact_time,
users.afternoon_time as ee_afternoon_time_contact_time,
users.account_activated as ee_account_activated_flag,
address.region_name ee_region_name,
city_name ee_city_name,
COALESCE(barangay_name) ee_barangay,
address.address_line_1 as ee_address_line_1,
address.address_line_2 as ee_address_line_2,
COALESCE(id_info.postal_code) ee_postal_code,
COALESCE(id_info.landmark,address.landmark) ee_landmark,
id_info.residing_date ee_residing_date,
income_info.employee_status as ee_employment_status,
users.civil_status ee_civil_status,
doc_type.name as ee_kyc_doc_name,
id_info.is_citizen ee_is_citizen_flag,
user_timelines.first_account_activated_at as ee_onboarding_date,
COALESCE(hired_date) as ee_employment_date,
customer_status as ee_fraud_status,
income_info.contract ee_job_type,
employer_employees.comment as ee_comment,
employer_employees.department as ee_department,
employer_employees.recommended_ir as ee_recommended_ir,
income_info.job_title as ee_job_title,
employment_ids.name as ee_employment_type,
income_info.nature_of_work as ee_nature_of_work,
freeze_tag.created_at as ee_permanent_freeze_date,
DATE(user_deleted_at) ee_resignation_date,
users.product_type as ee_product_type,
frozen_status as ee_frozen_status,
employer_time_score as cust_risk_employer_time_score_v1,
gender_score cust_risk_gender_score_v1,
declared_income_score cust_risk_declared_income_score_v1,
civil_status_score cust_risk_civil_status_score_v1,
(employer_time_score+gender_score+declared_income_score+civil_status_score) as cust_risk_combined_score_v1,
credit_rating cust_risk_cat_v1,
--employers related data
employers.id as er_employer_id,
employer_group.group_id as er_employer_group_id,
employers.name as er_employer_name,
employers.email_domain as er_email_domain,
employers.repayment_days er_repayment_days_month,
employers.custom_email as er_custom_email,
employers.payment_reminders as er_payment_reminders,
employers.address as er_address,
employers.postal_code_id as er_postal_code_id,
employers.max_bir as er_max_base_interest_rate,
employers.industry er_employer_industry,
CASE WHEN employers.status  = 1 THEN 'ACTIVATED'
WHEN employers.status  = 2 THEN 'IN_PROGRESS' 
WHEN employers.status  = 3 THEN 'SUSPENDED' 
WHEN employers.status  = 4 THEN 'PARKED' 
END AS er_employer_status,
employers.activated_at as er_activated_at,
employers.deleted_at as er_deleted_at,
employers.created_at as er_created_at,
employers.updated_at as er_updated_at,

--credit line data
kyc_credit_info.monthly_utility_bills_amount cl_monthly_utility_bills_amount,
income_info.verified_gross_income as cl_monthly_income_gross,
income_info.verified_net_income as cl_monthly_income_net,
CASE WHEN REGEXP_CONTAINS(model_has_permissions.model_type, r'App\\User') AND model_has_permissions.permission_id =2 THEN 1
ELSE 0
END AS cl_multiple_purchases_enabled_flag,
employers.max_credit_limit as cl_max_credit_limit_multiplier,
max_debt_income_ratio cl_max_debt_income_ratio,
cl_matured_fpd30_flag,
cl_matured_fspd30_flag,
cl_matured_fstpd30_flag,
cl_matured_fstfpd30_flag,
cl_fpd30_flag,
cl_fspd30_flag,
cl_fstpd30_flag,
cl_fstfpd30_flag,

--loan and repayment data
pr.id as ln_loan_id,
CASE WHEN pr.merchant_id = 423 THEN 'Tendo Plus'
when pr.id is not null then 'Tendo'
END AS ln_loan_type,
CASE 
WHEN pr.status = 'AUOK' THEN 'Authorized'
WHEN pr.status = 'PTOK' THEN 'Approved/Disbursed'
WHEN pr.status = 'CTOK' THEN 'Approved Transaction Cancelled'
WHEN pr.status = 'AUCA' THEN 'Authorization Cancelled'
WHEN pr.status = 'PTNG' THEN 'Rejected'
WHEN pr.status = 'PTCA' THEN 'Cancelled'
END AS ln_loan_status,
xendit_payment_responses.channel_code as ln_disbursement_channel,
pr.principal ln_original_principal, 
pr.fee ln_orig_interest_fees, 
pr.tendopay_installments as ln_orig_tenor,
pr.created_at ln_loan_application_datetime,
pr.repaid_full as ln_repaid_full_flag,
DATE(pr.fully_repaid_at) ln_fully_repaid_date,
CASE WHEN def_FPD30 = 1 THEN 1
ELSE 0
END ln_fpd30_flag,
CASE WHEN def_FPD30 = 1 OR def_SPD30 =1 THEN 1
ELSE 0
END ln_fspd30_flag,
CASE WHEN def_FPD30 = 1 OR def_SPD30 =1 OR def_TPD30 = 1 THEN 1
ELSE 0
END ln_fstpd30_flag,
CASE WHEN def_FPD30 = 1 OR def_SPD30 =1 OR def_TPD30 = 1 OR def_FSTFPD30 = 1 THEN 1
ELSE 0
END ln_fstfpd30_flag,
min_loan_due_date as ln_min_loan_due_date,
def_FPD30_vol AS ln_os_principal_at_fpd30,
def_SPD30_vol AS ln_os_principal_at_fspd30,
def_TPD30_vol AS ln_os_principal_at_fstpd30,
def_FSTFPD30_vol AS ln_os_principal_at_fstfpd30,

CASE WHEN DATE_DIFF(CURRENT_DATE(),min_loan_due_date,DAY) >= 30 THEN 1 ELSE 0 END AS ln_matured_fpd30_flag, 
CASE WHEN DATE_DIFF(CURRENT_DATE(),min_loan_due_date,DAY) >= 60 THEN 1 ELSE 0 END AS ln_matured_fspd30_flag, 
CASE WHEN DATE_DIFF(CURRENT_DATE(),min_loan_due_date,DAY) >= 90 THEN 1 ELSE 0 END AS ln_matured_fstpd30_flag, 
CASE WHEN DATE_DIFF(CURRENT_DATE(),min_loan_due_date,DAY) >= 120 THEN 1 ELSE 0 END AS ln_matured_fstfpd30_flag, 

from
tendopay_raw.users 
LEFT JOIN tendopay_raw.income_info on income_info.user_id = users.id
LEFT JOIN (SELECT * FROM tendopay_raw.employer_employees
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id order by updated_at desc) = 1) employer_employees
ON employer_employees.user_id = users.id --and employer_employees.user_deleted_at is null
LEFT JOIN (select document_ids.name, files.user_id
from tendopay_raw.document_ids
join tendopay_raw.files on files.doc_id = document_ids.type
where doc_type = 1 AND  REGEXP_CONTAINS(files.owner_type, r'App\\IdInfo') QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id order by updated_at DESC)=1 ) doc_type on doc_type.user_id = users.id
LEFT JOIN  tendopay_raw.id_info  on id_info.user_id = users.id
LEFT JOIN (select region.name as region_name,barangay.name as barangay_name,
address.user_id,address_line_1,address_line_2,landmark
FROM `tendopay_raw.address` address
LEFT JOIN tendopay_raw.barangay barangay ON barangay.id = address.barangay_id
LEFT JOIN tendopay_raw.cities_v2 AS city ON city.id = barangay.city_id
LEFT JOIN tendopay_raw.provinces_v2 AS province ON province.id = city.province_id
LEFT JOIN tendopay_raw.regions_v2 AS region ON region.id = province.region_id
where address.deleted_at IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY address.user_id ORDER BY created_at desc) = 1) address on address.user_id = users.id
LEFT JOIN  tendopay_raw.user_timelines  on user_timelines.user_id = users.id
LEFT JOIN  tendopay_raw.kyc_credit_info  on kyc_credit_info.user_id = users.id
LEFT JOIN tendopay_raw.employers on CAST(employers.id as string) = users.employer_id
LEFT JOIN tendopay_raw.employer_group ON employer_group.employer_id = employers.id
LEFT JOIN tendopay_raw.employment_ids ON employment_ids.id = income_info.employment_id
--LEFT JOIN tendopay_raw.tp_groups ON tp_groups.id = employer_group.group_id
LEFT JOIN tendopay_raw.payment_responses pr on pr.tendopay_user_id=users.id
LEFT JOIN tendopay_raw.model_has_permissions on model_has_permissions.model_id  = users.id
LEFT JOIN (SELECT model_id,JSON_EXTRACT_SCALAR(PARSE_JSON(OPTIONS),'$.max_debt_income_ratio') max_debt_income_ratio FROM tendopay_raw.model_has_tp_events WHERE tp_event = "E0000011") model_has_tp_events  ON model_has_tp_events.model_id = employers.id
LEFT JOIN tendopay_raw.xendit_payment_responses ON xendit_payment_responses.reference_id  = pr.merchant_order_id
LEFT JOIN (
SELECT 
user_id,
CASE WHEN SUM(CASE WHEN tag_id IN (2,111,113,112,39,106,107,102,100,101,103) AND deleted_at is null THEN 1 ELSE NULL END) > 0  THEN 'Risk'
ELSE 'Normal'
end as customer_status
from
tendopay_raw.user_tag
GROUP BY user_id
) user_tag on user_tag.user_id = users.id
LEFT JOIN (
SELECT user_id,created_at
from
tendopay_raw.user_tag
where tag_name = 'FREEZE_PERMANENT' and deleted_at is null
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id order by updated_at desc) = 1
) freeze_tag on freeze_tag.user_id = users.id
LEFT JOIN payment_channel_data ON payment_channel_data.loan_id = pr.id
--LEFT JOIN tendopay_raw.payment_requests on payment_requests.user_id = users.id
--LEFT JOIN tendopay_raw.payment_response_failures on payment_response_failures.payment_request_id = payment_requests.id
LEFT JOIN fspd_data ON fspd_data.loan_id = pr.id
LEFT JOIN rating on rating.user_id = users.id
LEFT JOIN cl_fspd_data ON cl_fspd_data.user_id = users.id
;