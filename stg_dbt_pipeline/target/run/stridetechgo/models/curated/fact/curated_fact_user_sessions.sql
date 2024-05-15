
  
    
        create or replace table `dev`.`dbt-nstankus_curated`.`curated_fact_user_sessions`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    user_session_no,
    user_session_length,
    facility_id,
    device_id,
    user_id,
    session_id,
    time_id,
    week_id,
    user_session_start_time,
    user_session_end_time,
    file_path,
    extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`processed`.`fact_user_sessions`
  