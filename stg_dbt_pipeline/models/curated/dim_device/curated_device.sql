SELECT 
    device_id,
    device_name,
    fsr_length,
    left_side_fsr_length,
    right_side_fsr_length,
    width,
    file_path,
    entered_by,
    extraction_time,
    current_timestamp() AS last_updated
FROM {{ source('warehouse', 'device')}}