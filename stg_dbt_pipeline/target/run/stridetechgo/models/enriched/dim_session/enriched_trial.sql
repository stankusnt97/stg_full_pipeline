
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_trial`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT DISTINCT
    ssh.session_id,
    session_name,
    session_type AS trial_type,
    session_description,
    session_start_time,
    session_end_time,
    ssh.file_path,
    ssh.extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`dbt-nstankus_curated`.`curated_session` ssh
JOIN `dev`.`dbt-nstankus_curated`.`curated_fact_stg` stg
    ON ssh.session_id = stg.session_id
  