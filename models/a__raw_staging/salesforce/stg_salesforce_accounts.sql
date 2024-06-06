with 

source as (

    select * from {{ source('salesforce', 'accounts') }}

),

renamed as (

    select
        id as account_id,
        is_deleted,
        master_record_id,
        name as company_name,
        type,
        parent_id,
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        billing_country,
        billing_latitude,
        billing_longitude,
        billing_geocode_accuracy,
        shipping_street,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        shipping_latitude,
        shipping_longitude,
        shipping_geocode_accuracy,
        phone,
        fax,
        account_number,
        website,
        photo_url,
        sic,
        industry,
        annual_revenue,
        number_of_employees,
        ownership,
        ticker_symbol,
        description,
        rating,
        site,
        owner_id,
        created_date,
        created_by_id,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        last_activity_date,
        last_viewed_date,
        last_referenced_date,
        jigsaw,
        jigsaw_company_id,
        clean_status,
        account_source,
        duns_number,
        tradestyle,
        naics_code,
        naics_desc,
        year_started,
        sic_desc,
        dandb_company_id,
        operating_hours_id,
        customer_priority_c,
        sla_c,
        active_c,
        numberof_locations_c,
        upsell_opportunity_c,
        slaserial_number_c,
        slaexpiration_date_c,
        deleted_datetimestamp,
        created_datetimestamp,
        modified_datetimestamp

    from source

)

select * from renamed
