
  
    
        create or replace table `dev`.`dbt-nstankus_curated`.`curated_week`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
week_id,
week_name,
week_no,
file_path,
extraction_time,
current_timestamp() AS last_updated
FROM `prod`.`processed`.`week`
  