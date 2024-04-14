select
    left_misuse_flag,
    right_misuse_flag,
    hip_misuse_flag,
    total_misuse_flag,
    -- Create metrics to track difference between identified misuse and triggered misuse
    --- Missed triggers
    CASE 
        WHEN left_vibration_trigger > 1 AND left_misuse_flag = 1 THEN 0
            WHEN left_vibration_trigger > 1 AND left_misuse_flag = 0 THEN 0
                WHEN left_vibration_trigger = 1 AND left_misuse_flag = 1 THEN 1
        ELSE NULL
    END AS left_missed_trigger,
    CASE 
        WHEN right_vibration_trigger > 1 AND right_misuse_flag = 1 THEN 0
            WHEN right_vibration_trigger > 1 AND right_misuse_flag = 0 THEN 0
                WHEN right_vibration_trigger = 1 AND right_misuse_flag = 1 THEN 1
        ELSE NULL
    END AS right_missed_trigger,
    CASE 
        WHEN hip_vibration_trigger > 1 AND hip_misuse_flag = 1 THEN 0
            WHEN hip_vibration_trigger > 1 AND hip_misuse_flag = 0 THEN 0
                WHEN hip_vibration_trigger = 1 AND hip_misuse_flag = 1 THEN 1
        ELSE NULL
    END AS hip_missed_trigger,
    CASE 
        WHEN (left_vibration_trigger > 1 OR right_vibration_trigger > 1 OR hip_vibration_trigger > 1) AND total_misuse_flag = 1 THEN 0
            WHEN (left_vibration_trigger > 1 OR right_vibration_trigger > 1 OR hip_vibration_trigger > 1) AND total_misuse_flag = 0 THEN 0
                WHEN (left_vibration_trigger = 1 AND right_vibration_trigger = 1 AND hip_vibration_trigger = 1) AND total_misuse_flag = 1 THEN 1
        ELSE NULL
    END AS total_missed_trigger,
    --- Trigger misfires
    CASE 
        WHEN left_vibration_trigger = 3 AND left_misuse_flag = 1 THEN 0
            WHEN left_vibration_trigger = 3 AND left_misuse_flag = 0 THEN 1
                WHEN left_vibration_trigger < 3 AND left_misuse_flag = 1 THEN 0
        ELSE NULL
    END AS left_trigger_misfire,
    CASE 
        WHEN right_vibration_trigger = 3 AND right_misuse_flag = 1 THEN 0
            WHEN right_vibration_trigger = 3 AND right_misuse_flag = 0 THEN 1
                WHEN right_vibration_trigger < 3 AND right_misuse_flag = 1 THEN 0
        ELSE NULL
    END AS right_trigger_misfire,
    CASE 
        WHEN hip_vibration_trigger = 3 AND hip_misuse_flag = 1 THEN 0
            WHEN hip_vibration_trigger = 3 AND hip_misuse_flag = 0 THEN 1
                WHEN hip_vibration_trigger < 3 AND hip_misuse_flag = 1 THEN 0
        ELSE NULL
    END AS hip_trigger_misfire,
    CASE 
        WHEN (left_vibration_trigger = 3 OR right_vibration_trigger = 3 OR hip_vibration_trigger = 3) AND total_misuse_flag = 1 THEN 0
            WHEN (left_vibration_trigger = 3 OR right_vibration_trigger = 3 OR hip_vibration_trigger = 3) AND total_misuse_flag = 0 THEN 1
                WHEN (left_vibration_trigger < 3 AND right_vibration_trigger < 3 AND hip_vibration_trigger < 3) AND total_misuse_flag = 1 THEN 0
        ELSE NULL
    END AS total_trigger_misfire,
    -- Translate triggers to vibration
    CASE 
        WHEN left_vibration_trigger == 3 AND activity_flag = 1 THEN 1
            WHEN left_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS left_vibration_flag,
    CASE 
        WHEN right_vibration_trigger == 3 AND activity_flag = 1 THEN 1
            WHEN right_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS right_vibration_flag,
    CASE 
        WHEN hip_vibration_trigger == 3 AND activity_flag = 1 THEN 1
            WHEN hip_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS hip_vibration_flag,
    CASE 
        WHEN (left_vibration_trigger == 3 OR right_vibration_trigger == 3 OR hip_vibration_trigger == 3) AND activity_flag = 1 THEN 1
            WHEN COALESCE(left_vibration_flag, right_vibration_trigger, hip_vibration_trigger) IS NOT NULL THEN 0
            ELSE NULL
    END AS vibration_flag,
    e.activity_flag AS activity_sec,
    left_lbf,
    right_lbf,
    left_adc,
    right_adc,
    hip_distance,
    force_threshold,
    hip_distance_threshold,
    baseline_left_adc,
    baseline_right_adc,
    left_adc_change,
    right_adc_change,
    fsr_length_adjustment,
    -- PT metrics
    start_abc_score,
    start_tug_score_fastest_attempt,
    start_tandem_balance_total_time,
    start_successful_tandem_balance_positions,
    start_functional_reach,
    end_abc_score,
    end_tug_score_fastest_attempt,
    end_tandem_balance_score_total,
    end_successful_tandem_balance_positions,
    end_functional_reach,
    -- Survey metrics
    self_reported_physical_activity_per_week_hrs,
    satisfied_with_activity,
    fear_of_falling,
    fall_history,
    length_of_time_using_walker,
    likes_using_walker,
    received_walker_training,
    description_of_changes_in_DLa,
    would_purchase_stg,
    how_much_would_you_pay,
    would_recommend_stg,
    stg_helped_learn_use_walker_better,
    u.user_id,
    h.hour_id,
    t.trial_id,
    d.device_id,
    e.user_time,
    e.extraction_time
from `dev`.`dbt-nstankus_curated`.`curated_fact_stg` e
join `dev`.`dbt-nstankus_enriched`.`enriched_user` u
    on u.user = e.user
join `dev`.`dbt-nstankus_enriched`.`enriched_hour` h
    on h.week = e.week
    and h.hour = date_trunc('hour', e.user_time)
join `dev`.`dbt-nstankus_enriched`.`enriched_trial` t
    on t.trial_type = e.trial_type
join `dev`.`dbt-nstankus_enriched`.`enriched_device` d 
    on d.device = e.device
left join `dev`.`dbt-nstankus_curated`.`curated_fact_physical_therapy_evals` pt
    on pt.user_id = u.user_id
    and pt.device_id = d.device_id
left join `dev`.`dbt-nstankus_curated`.`curated_fact_user_surveys` us
    on us.user_id = u.user_id
    and us.device_id = d.device_id
-- Flag for inactivity
where e.activity_flag > 0