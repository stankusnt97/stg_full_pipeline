
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_trial`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT 
    trial_type,
    MD5(trial_type) AS trial_id,
    current_timestamp() AS lastupdated
FROM `dev`.`dbt-nstankus_curated`.`curated_fact_stg`
GROUP BY 
    trial_type,
    MD5(trial_type)
  