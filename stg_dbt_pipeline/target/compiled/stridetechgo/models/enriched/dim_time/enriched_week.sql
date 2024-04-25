SELECT DISTINCT
w.week_id,
w.week_name,
w.week_no,
w.file_path,
w.extraction_time,
current_timestamp() AS last_updated
FROM `prod`.`curated`.`curated_week` w
JOIN `prod`.`curated`.`curated_fact_stg` stg
    ON stg.week_id = w.week_id