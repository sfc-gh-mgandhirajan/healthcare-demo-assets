{{
    config(
        alias='MEDICAL_CLAIMS_LINES'
    )
}}

with lines as (
    select * from {{ ref('stg_claims_lines') }}
),

claims as (
    select 
        claim_id,
        member_id,
        payer_name,
        provider_id,
        claim_status,
        service_date as claim_service_date
    from {{ ref('stg_claims_header') }}
)

select
    l.line_id,
    l.claim_id,
    c.member_id,
    c.payer_name,
    c.provider_id,
    l.line_number,
    l.procedure_code,
    l.procedure_description,
    l.modifier,
    l.quantity,
    l.billed_amount,
    l.allowed_amount,
    l.paid_amount,
    l.service_from_date,
    l.service_to_date,
    l.place_of_service,
    case l.place_of_service
        when '11' then 'Office'
        when '21' then 'Inpatient Hospital'
        when '22' then 'Outpatient Hospital'
        when '23' then 'Emergency Room'
        else 'Other'
    end as place_of_service_description,
    c.claim_status,
    case 
        when l.billed_amount > 0 then round((l.billed_amount - l.allowed_amount) / l.billed_amount * 100, 2)
        else 0
    end as discount_pct,
    current_timestamp() as loaded_at
from lines l
left join claims c on l.claim_id = c.claim_id
