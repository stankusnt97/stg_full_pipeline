
  
    
        create or replace table `dev`.`dbt-nstankus_curated`.`curated_session`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    session_id,
    session_name,
    session_type,
    session_description,
    session_start_time,
    session_end_time,
    file_path,
    
    extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`processed`.`session`
  