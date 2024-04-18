{{ config(materialized='view') }}

with contact_details as (

    select *
    from {{ ref('opportunities_accounts_contacts_details') }}

), aggragate_data as (

    select
        company_name,
        concat_ws(' ', first_name, last_name) as contact_full_name,
        sum(amount) as opportunity_amount
    from contact_details
    where company_name is not null
        group by
            company_name,
            concat_ws(' ', first_name, last_name)

)

select *
from aggragate_data
