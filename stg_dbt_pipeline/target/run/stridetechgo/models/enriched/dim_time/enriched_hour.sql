
  
    
        create or replace table `prod`.`enriched`.`enriched_hour`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT DISTINCT
t.time_id,
t.hour,
t.day,
t.file_path,
t.extraction_time,
current_timestamp() AS last_updated
FROM `prod`.`curated`.`curated_time` t
JOIN `prod`.`curated`.`curated_fact_stg` stg
    ON stg.time_id = t.time_id
  