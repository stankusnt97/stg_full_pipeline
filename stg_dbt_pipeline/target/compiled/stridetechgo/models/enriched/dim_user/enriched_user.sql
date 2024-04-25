SELECT DISTINCT
    u.user_id,
    u.user_alias,
    RIGHT(u.user_alias, LENGTH(u.user_alias) - 1) AS user_no,
    u.physical_therapist_name,
    u.walker_purchased_from,
    u.walker_manufacturer,
    u.self_reported_activity_level,
    u.satisfied_with_activity,
    u.fear_of_falling,
    u.fall_history,
    u.length_of_time_using_walker,
    u.length_of_time_using_walker_units,
    u.likes_using_walker,
    u.received_walker_training,
    u.description_of_changes,
    u.would_purchase,
    u.would_pay_how_much,
    u.would_recommend,
    u.improved_walker_use,
    u.file_path,
    u.extraction_time,
    current_timestamp() AS last_updated
FROM `prod`.`curated`.`curated_user` u
JOIN `prod`.`curated`.`curated_fact_stg` stg
    ON u.user_id = stg.user_id