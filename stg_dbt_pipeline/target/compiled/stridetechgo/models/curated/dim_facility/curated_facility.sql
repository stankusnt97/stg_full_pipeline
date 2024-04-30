SELECT 
    facility_id,
    facility_name,
    facility_contact,
    file_path,
    
    extraction_time,
    current_timestamp() AS last_updated
FROM `prod`.`processed`.`facility`