SELECT 
    facility,
    md5(facility) AS facility_id,
    current_timestamp() AS lastupdated
FROM `dev`.`integrated`.`physical_therapy_evals`
GROUP BY 
    facility,
    md5(facility)