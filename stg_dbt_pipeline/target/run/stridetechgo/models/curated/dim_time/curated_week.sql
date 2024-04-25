
  
    
        create or replace table `prod`.`curated`.`curated_week`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
week_id,
week_name,
week_no,
file_path,
extraction_time,
current_timestamp() AS last_updated
FROM `dev`.`processed`.`week`
  