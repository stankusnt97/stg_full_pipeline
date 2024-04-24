SELECT 
    session_id,
    session_name,
    session_type,
    session_description,
    session_start_time,
    session_end_time,
    file_path,
    entered_by,
    extraction_time,
    current_timestamp() AS last_updated
FROM {{ source('warehouse', 'session') }}
