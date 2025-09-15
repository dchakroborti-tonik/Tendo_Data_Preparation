WITH paid_payments as (   ----------------more payments belonging one repayment
select repayment_schedule_id,
sum(amount) as amount,
max(repaid_date) as repaid_date
from tendopay_raw.split_purchases
group by  repayment_schedule_id
), 
payment_channel_data as (
SELECT
u.id as user_id,
pr.id AS loan_id,
DENSE_RANK() over (partition by pr.id order by rs.due_date ) as installment_number,
vendor_id,
subfamily,
split_purchases.repaid_date,
rs.due_date,
rs.principal as due_principal,
rs.interest as due_interest,
rs.amount as due_amount,
split_purchases.principal as paid_principal,
split_purchases.interest as paid_interest,
split_purchases.amount as paid_amount,
outstanding_balance,
case
     when (pp.repaid_date is not null and rs.amount <= pp.amount) then date_diff(pp.repaid_date, rs.due_date, day)
     when (pp.repaid_date is not null and rs.amount > pp.amount) then date_diff(CURRENT_DATE, rs.due_date, day)
     when pp.repaid_date is null and due_date < CURRENT_DATE then date_diff(CURRENT_DATE, rs.due_date, day)
end as DPD

from tendopay_raw.payment_responses pr 
JOIN (select * from tendopay_raw.users where is_delete is null) u on u.id=pr.tendopay_user_id
JOIN (select * from tendopay_raw.repayment_schedules where is_delete is null) rs ON rs.payment_response_id = pr.id
JOIN (select * from tendopay_raw.split_purchases where is_delete is null) split_purchases  ON cast(rs.id as string) = split_purchases.repayment_schedule_id AND CAST(split_purchases.repaid_date AS date) < CURRENT_DATE()
JOIN `tendopay_raw.customer_repayment_responses` crs ON split_purchases.txnid = crs.txn_id
join paid_payments pp on cast(rs.id as string)=pp.repayment_schedule_id
where u.product_type = 'employer'
),
ordered_payments AS (
  SELECT *,
         ROW_NUMBER() OVER (
            PARTITION BY user_id, loan_id, installment_number 
            ORDER BY repaid_date, due_date
         ) AS payment_seq
  FROM payment_channel_data
),
balance_calc AS (
  SELECT *,
         -- Get the previous installment's last known outstanding balance
         LAG(outstanding_balance) OVER (
            PARTITION BY user_id, loan_id 
            ORDER BY installment_number
         ) AS prev_installment_balance
  FROM ordered_payments
),
before_calc AS (
  SELECT *,
         -- Determine Outstanding Before
         CASE 
           -- First payment of an installment: Use previous installment's balance
           WHEN payment_seq = 1 THEN COALESCE(prev_installment_balance, outstanding_balance + paid_amount)
           -- Subsequent payments in the same installment: Use previous row's outstanding_after
           ELSE LAG(outstanding_balance) OVER (
               PARTITION BY user_id, loan_id, installment_number 
               ORDER BY repaid_date, due_date
           )
         END AS outstanding_before
  FROM balance_calc
)
SELECT *,
FROM payment_channel_data
--where loan_id = 3972
ORDER BY user_id, loan_id, installment_number, repaid_date;
