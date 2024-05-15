
  
    
        create or replace table `dev`.`dbt-nstankus_enriched`.`enriched_fact_stg`
      
      
    using delta
      
      
      
      
      
      
      
      as
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
        WHEN left_vibration_trigger == 3  THEN 1
            WHEN left_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS left_vibration_flag,
    CASE 
        WHEN right_vibration_trigger == 3  THEN 1
            WHEN right_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS right_vibration_flag,
    CASE 
        WHEN hip_vibration_trigger == 3  THEN 1
            WHEN hip_vibration_trigger IS NOT NULL THEN 0
            ELSE NULL
    END AS hip_vibration_flag,
    CASE 
        WHEN (left_vibration_trigger == 3 OR right_vibration_trigger == 3 OR hip_vibration_trigger == 3)  THEN 1
            WHEN COALESCE(left_vibration_flag, right_vibration_trigger, hip_vibration_trigger) IS NOT NULL THEN 0
            ELSE NULL
    END AS vibration_flag,
    activity_flag,
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
    d.fsr_length,
    -- Session metrics
    uses.user_session_no,
    uses.user_session_length,
    uses.user_session_start_time,
    uses.user_session_end_time,
    -- PT metrics
    pt.start_abc_score,
    pt.start_tug_score_fastest_attempt,
    pt.start_tandem_balance_total_time,
    pt.start_successful_tandem_balance_positions,
    pt.start_functional_reach,
    pt.end_abc_score,
    pt.end_tug_score_fastest_attempt,
    pt.end_tandem_balance_score_total,
    pt.end_successful_tandem_balance_positions,
    pt.end_functional_reach,
    -- Survey metrics
    us.self_reported_physical_activity_per_week_hrs,
    us.satisfied_with_activity,
    us.fear_of_falling,
    us.fall_history,
    us.length_of_time_using_walker,
    us.likes_using_walker,
    us.received_walker_training,
    us.description_of_changes_in_DLa,
    us.would_purchase_stg,
    us.how_much_would_you_pay,
    us.would_recommend_stg,
    us.stg_helped_learn_use_walker_better,
    f.facility_id,
    u.user_id,
    t.time_id,
    w.week_id,
    s.session_id,
    d.device_id,
    e.extraction_time,
    e.device_time,
    current_timestamp() AS last_updated
from `dev`.`dbt-nstankus_curated`.`curated_fact_stg` e
join `dev`.`dbt-nstankus_curated`.`curated_user` u
    on u.user_id = e.user_id
join `dev`.`dbt-nstankus_curated`.`curated_time` t
    on t.time_id = e.time_id
join `dev`.`dbt-nstankus_curated`.`curated_week` w
    on w.week_id = e.week_id
join `dev`.`dbt-nstankus_curated`.`curated_session` s
    on s.session_id = e.session_id
join `dev`.`dbt-nstankus_curated`.`curated_device` d 
    on d.device_id = e.device_id
join `dev`.`dbt-nstankus_curated`.`curated_facility` f
    on f.facility_id = e.facility_id
left join `dev`.`dbt-nstankus_curated`.`curated_fact_physical_therapy_evals` pt
    on pt.user_id = u.user_id
    and pt.device_id = d.device_id
left join `dev`.`dbt-nstankus_curated`.`curated_fact_user_surveys` us
    on us.user_id = u.user_id
    and us.device_id = d.device_id
left join `dev`.`dbt-nstankus_curated`.`curated_fact_user_sessions` uses
    on uses.user_id = e.user_id
    and e.device_time between uses.user_session_start_time and uses.user_session_end_time
where activity_flag = 1
  