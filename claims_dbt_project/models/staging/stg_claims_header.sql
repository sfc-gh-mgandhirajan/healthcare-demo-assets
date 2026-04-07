with source as (
    select * from {{ source('raw_claims', 'RAW_CLAIMS_HEADER') }}
),

transformed as (
    select
        claim_id,
        member_id,
        payer_name,
        provider_id,
        provider_name,
        provider_type,
        try_to_date(service_date) as service_date,
        try_to_date(submitted_date) as submitted_date,
        primary_diagnosis_code,
        primary_diagnosis_description,
        nullif(secondary_diagnosis_code, '') as secondary_diagnosis_code,
        nullif(secondary_diagnosis_description, '') as secondary_diagnosis_description,
        try_to_decimal(total_billed_amount, 18, 2) as total_billed_amount,
        try_to_decimal(total_allowed_amount, 18, 2) as total_allowed_amount,
        try_to_decimal(total_paid_amount, 18, 2) as total_paid_amount,
        claim_status,
        try_to_number(num_lines) as num_lines
    from source
)

select * from transformed
