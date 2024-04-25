SELECT DISTINCT
t.time_id,
t.hour,
t.day,
t.file_path,
t.extraction_time,
current_timestamp() AS last_updated
FROM {{ ref('curated_time') }} t
JOIN {{ ref('curated_fact_stg') }} stg
    ON stg.time_id = t.time_id
