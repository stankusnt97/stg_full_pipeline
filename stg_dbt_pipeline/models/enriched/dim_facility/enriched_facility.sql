SELECT DISTINCT
    COALESCE(f.facility_name, pte.facility_name) AS facility,
    f.facility_id,
    f.facility_contact,
    f.file_path,
    f.extraction_time,
    current_timestamp() AS last_updated
FROM {{ ref('curated_facility') }} f
JOIN {{ ref('curated_fact_stg') }} stg
    ON stg.facility_id = f.facility_id
LEFT JOIN {{ source('integrated_warehouse', 'physical_therapy_evals')}} pte
    ON f.facility_name = pte.facility_name