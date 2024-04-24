SELECT 
week_id,
week_name,
week_no,
file_path,
entered_by,
extraction_time,
current_timestamp() AS last_updated
FROM {{ source('warehouse', 'week')}}