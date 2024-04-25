SELECT 
    NULL AS start_week,
    NULL AS end_week,
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
    d.device_id,
    u.user_id,
    fac.facility_id,
    current_timestamp() AS last_updated
from `prod`.`integrated`.`physical_therapy_evals` pt
left join `prod`.`curated`.`curated_device` d
    on d.device_name = pt.device_name
left join `prod`.`curated`.`curated_user` u
    on u.user_alias = pt.user_alias
left join `prod`.`curated`.`curated_facility` fac
    on fac.facility_name = pt.facility_name