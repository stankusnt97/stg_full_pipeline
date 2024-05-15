
  
    
        create or replace table `dev`.`dbt-nstankus_curated`.`curated_facility`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    facility_id,
    facility_name,
    facility_contact,
    file_path,
    
    extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`processed`.`facility`
  