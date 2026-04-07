{{
    config(
        alias='MEDICAL_CLAIMS'
    )
}}

with claims as (
    select * from {{ ref('stg_claims_header') }}
),

lines_agg as (
    select
        claim_id,
        count(*) as actual_line_count,
        sum(billed_amount) as sum_line_billed,
        sum(allowed_amount) as sum_line_allowed,
        sum(paid_amount) as sum_line_paid
    from {{ ref('stg_claims_lines') }}
    group by claim_id
)

select
    c.claim_id,
    c.member_id,
    c.payer_name,
    c.provider_id,
    c.provider_name,
    c.provider_type,
    c.service_date,
    c.submitted_date,
    c.primary_diagnosis_code,
    c.primary_diagnosis_description,
    c.secondary_diagnosis_code,
    c.secondary_diagnosis_description,
    c.total_billed_amount,
    c.total_allowed_amount,
    c.total_paid_amount,
    c.claim_status,
    coalesce(la.actual_line_count, 0) as line_count,
    case 
        when c.total_billed_amount > 0 then round(c.total_paid_amount / c.total_billed_amount * 100, 2)
        else 0 
    end as payment_ratio_pct,
    datediff('day', c.service_date, c.submitted_date) as days_to_submit,
    current_timestamp() as loaded_at
from claims c
left join lines_agg la on c.claim_id = la.claim_id
