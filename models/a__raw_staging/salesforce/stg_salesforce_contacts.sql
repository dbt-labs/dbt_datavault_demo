with 

source as (

    select * from {{ source('salesforce', 'contacts') }}

),

renamed as (

    select
        id as contact_id,
        is_deleted,
        master_record_id,
        account_id,
        l_name,
        f_name,
        salutation,
        name,
        other_street,
        other_city,
        other_state,
        other_postal_code,
        other_country,
        other_latitude,
        other_longitude,
        other_geocode_accuracy,
        mailing_street,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_country,
        mailing_latitude,
        mailing_longitude,
        mailing_geocode_accuracy,
        phone,
        fax,
        mobile_phone,
        home_phone,
        other_phone,
        assistant_phone,
        reports_to_id,
        email,
        title,
        department,
        assistant_name,
        lead_source,
        birthdate,
        description,
        owner_id,
        created_date,
        created_by_id,
        last_modified_date,
        last_modified_by_id,
        system_modstamp,
        last_activity_date,
        last_curequest_date,
        last_cuupdate_date,
        last_viewed_date,
        last_referenced_date,
        email_bounced_reason,
        email_bounced_date,
        is_email_bounced,
        photo_url,
        jigsaw,
        jigsaw_contact_id,
        clean_status,
        individual_id,
        level_c,
        languages_c,
        deleted_datetimestamp,
        created_datetimestamp,
        modified_datetimestamp

    from source

)

select * from renamed
