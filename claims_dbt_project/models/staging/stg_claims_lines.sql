with source as (
    select * from {{ source('raw_claims', 'RAW_CLAIMS_LINES') }}
),

transformed as (
    select
        line_id,
        claim_id,
        try_to_number(line_number) as line_number,
        procedure_code,
        procedure_description,
        nullif(modifier, '') as modifier,
        try_to_number(quantity) as quantity,
        try_to_decimal(billed_amount, 18, 2) as billed_amount,
        try_to_decimal(allowed_amount, 18, 2) as allowed_amount,
        try_to_decimal(paid_amount, 18, 2) as paid_amount,
        try_to_date(service_from_date) as service_from_date,
        try_to_date(service_to_date) as service_to_date,
        place_of_service
    from source
)

select * from transformed
