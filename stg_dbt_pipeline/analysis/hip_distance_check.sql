
WITH Misuse_pct_check AS (
-- Retrieve hip mistakes for users
SELECT 
    --ROUND(hip_distance), 
    --u.user_alias, 
    w.week_name,
    AVG(hip_distance) AS hip_distance,
    SUM(hip_misuse_flag) AS hip_misuse_flags, 
    SUM(hip_vibration_flag) AS hip_vibration_flag, 
    COUNT(1) AS total_captured_sec, 
    SUM(total_misuse_flag) AS total_misuse_flag,
    (SUM(left_misuse_flag) / COUNT(*)) AS left_misuse_percentage,
    (SUM(left_misuse_flag) / COUNT(*)) AS left_misuse_percentage_week_total,
    (SUM(right_misuse_flag) / COUNT(*)) AS right_misuse_percentage,
    (SUM(right_misuse_flag) / COUNT(*)) AS right_misuse_percentage_week_total,
    (SUM(hip_misuse_flag) / COUNT(*)) AS hip_misuse_percentage,
    (SUM(hip_misuse_flag) / COUNT(*)) AS hip_misuse_percentage_week_total,
    (SUM(total_misuse_flag) / COUNT(*) ) AS total_misuse_percentage,
    (SUM(total_misuse_flag) / COUNT(*)) AS total_misuse_week_total
FROM {{ ref( 'enriched_fact_stg') }} stg
JOIN {{ ref( 'enriched_user' ) }} u ON 
    u.user_id = stg.user_id
JOIN {{ ref( 'enriched_week' )}} w ON 
    w.week_id = stg.week_id
JOIN {{ ref('enriched_device')}} d ON 
    d.device_id = stg.device_id
WHERE w.week_no IN (7, 12)
AND user_alias = 'U586092'
GROUP BY 
    w.week_name,
    w.week_no
)




select 
    ( 0.0007866184207368615/0.002392913932561798) - 1 AS left_decrease_misuse,
    (0.0001293254013753823/ 0.0004859065960764099) - 1 AS right_decrease_misuse


-- Find User/Device Pairs
SELECT 
    user_alias,
    device_name
FROM {{ ref('enriched_fac_stg')}} stg
JOIN {{ ref('enriched_user')}} u 
    ON u.user_id = stg.user_id
JOIN {{ ref('enriched_device')}} d 
    ON d.device_id = stg.device_id
GROUP BY 1, 2


SELECT hip_misuse_flag
FROM {{ ref('enriched_fact_stg' )}}
GROUP BY hip_misuse_flag