SELECT 
    facility_id,
    facility_name,
    facility_contact,
    file_path,
    entered_by,
    extraction_time,
    current_timestamp() AS last_updated
FROM {{ source('warehouse', 'facility')}}