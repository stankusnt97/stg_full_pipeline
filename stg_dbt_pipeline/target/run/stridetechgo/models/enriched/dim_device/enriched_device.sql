
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_device`
      
      
    using delta
      
      
      
      
      
      
      
      as
      SELECT DISTINCT
    dev.device_id AS device_id,
    dev.device_name AS device,
    dev.fsr_length AS fsr_length,
    dev.left_side_fsr_length AS left_side_fsr_length,
    dev.right_side_fsr_length AS right_side_fsr_length,
    dev.width,
    dev.file_path,
    dev.extraction_time,
    current_timestamp() AS last_updated
FROM `dev`.`dbt-nstankus_curated`.`curated_device` dev
LEFT JOIN `dev`.`dbt-nstankus_curated`.`curated_fact_stg` stg
    ON stg.device_id = dev.device_id
LEFT JOIN `dev`.`integrated`.`physical_therapy_evals` pt
    ON pt.device_name = dev.device_name
LEFT JOIN `dev`.`integrated`.`user_surveys` s
    ON s.device_name = dev.device_name
  