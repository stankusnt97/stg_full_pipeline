
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_hour`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT DISTINCT
t.time_id,
t.hour,
t.day,
t.file_path,
t.extraction_time,
current_timestamp() AS last_updated
FROM `dev`.`dbt-nstankus_curated`.`curated_time` t
JOIN `dev`.`dbt-nstankus_curated`.`curated_fact_stg` stg
    ON stg.time_id = t.time_id
  