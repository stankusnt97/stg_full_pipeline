SELECT 
time_id,
hour,
day,
file_path,
entered_by,
extraction_time,
current_timestamp() AS last_updated
FROM {{ source('warehouse', 'time') }}