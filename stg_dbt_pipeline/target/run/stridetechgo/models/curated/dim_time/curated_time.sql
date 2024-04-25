
  
    
        create or replace table `prod`.`curated`.`curated_time`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
time_id,
hour,
day,
file_path,

extraction_time,
current_timestamp() AS last_updated
FROM `dev`.`processed`.`time`
  