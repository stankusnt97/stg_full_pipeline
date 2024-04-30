-- Find User/Device Pairs
SELECT 
    user_alias,
    device
FROM {{ ref('enriched_fact_stg_hourly')}} stg
JOIN {{ ref('enriched_user')}} u 
    ON u.user_id = stg.user_id
JOIN {{ ref('enriched_device')}} d 
    ON d.device_id = stg.device_id
GROUP BY 1, 2