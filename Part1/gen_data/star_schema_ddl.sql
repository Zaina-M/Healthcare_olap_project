
INSERT INTO star_schema.dim_date (
    date_key,
    calendar_date,
    day_of_month,
    month,
    month_name,
    quarter,
    year,
    is_weekend
)
SELECT DISTINCT
    DATE_FORMAT(d, '%Y%m%d') AS date_key,
    d AS calendar_date,
    DAY(d),
    MONTH(d),
    MONTHNAME(d),
    QUARTER(d),
    YEAR(d),
    CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END
FROM (
    SELECT DATE(encounter_date) d FROM production.encounters
    UNION
    SELECT DATE(discharge_date) FROM production.encounters
    UNION
    SELECT procedure_date FROM production.encounter_procedures
    UNION
    SELECT claim_date FROM production.billing
) dates
WHERE d IS NOT NULL
ON DUPLICATE KEY UPDATE calendar_date = VALUES(calendar_date);


# 2️. SPECIALTY DIMENSION

INSERT INTO star_schema.dim_specialty (
    specialty_id,
    specialty_name,
    specialty_code
)
SELECT
    specialty_id,
    specialty_name,
    specialty_code
FROM production.specialties
ON DUPLICATE KEY UPDATE 
    specialty_name = VALUES(specialty_name),
    specialty_code = VALUES(specialty_code);


# 3️. DEPARTMENT DIMENSION


INSERT INTO star_schema.dim_department (
    department_id,
    department_name,
    floor,
    capacity
)
SELECT
    department_id,
    department_name,
    floor,
    capacity
FROM production.departments
ON DUPLICATE KEY UPDATE 
    department_name = VALUES(department_name),
    floor = VALUES(floor),
    capacity = VALUES(capacity);


# 4️. ENCOUNTER TYPE DIMENSION


INSERT INTO star_schema.dim_encounter_type (
    encounter_type_code,
    encounter_type_description
)
SELECT DISTINCT
    encounter_type,
    CONCAT(encounter_type, ' Encounter')
FROM production.encounters
ON DUPLICATE KEY UPDATE encounter_type_description = VALUES(encounter_type_description);


# 5️. PATIENT DIMENSION (derived attributes)


INSERT INTO star_schema.dim_patient (
    patient_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    age,
    age_group,
    mrn
)
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.date_of_birth,
    TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) AS age,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN 'Child'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 18 AND 39 THEN 'Adult'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 40 AND 64 THEN 'Middle Age'
        ELSE 'Senior'
    END AS age_group,
    p.mrn
FROM production.patients p
ON DUPLICATE KEY UPDATE 
    first_name = VALUES(first_name),
    last_name = VALUES(last_name),
    gender = VALUES(gender),
    age = VALUES(age),
    age_group = VALUES(age_group);


# 6️. PROVIDER DIMENSION (key mapping)


INSERT INTO star_schema.dim_provider (
    provider_id,
    provider_name,
    credential,
    specialty_key,
    department_key
)
SELECT
    pr.provider_id,
    CONCAT(pr.first_name, ' ', pr.last_name),
    pr.credential,
    ds.specialty_key,
    dd.department_key
FROM production.providers pr
JOIN star_schema.dim_specialty ds
    ON pr.specialty_id = ds.specialty_id
JOIN star_schema.dim_department dd
    ON pr.department_id = dd.department_id
ON DUPLICATE KEY UPDATE 
    provider_name = VALUES(provider_name),
    credential = VALUES(credential),
    specialty_key = VALUES(specialty_key),
    department_key = VALUES(department_key);


# 7️. DIAGNOSIS DIMENSION


INSERT INTO star_schema.dim_diagnosis (
    diagnosis_id,
    icd10_code,
    icd10_description
)
SELECT
    diagnosis_id,
    icd10_code,
    icd10_description
FROM production.diagnoses
ON DUPLICATE KEY UPDATE 
    icd10_code = VALUES(icd10_code),
    icd10_description = VALUES(icd10_description);


# 8️. PROCEDURE DIMENSION

INSERT INTO star_schema.dim_procedure (
    procedure_id,
    cpt_code,
    cpt_description
)
SELECT
    procedure_id,
    cpt_code,
    cpt_description
FROM production.procedures
ON DUPLICATE KEY UPDATE 
    cpt_code = VALUES(cpt_code),
    cpt_description = VALUES(cpt_description);


# 9️. FACT TABLE (THIS IS THE CORE)

-- This is where OLTP → OLAP transformation really happens.


INSERT INTO star_schema.fact_encounters (
    encounter_id,
    date_key,
    discharge_date_key,
    patient_key,
    provider_key,
    specialty_key,
    department_key,
    encounter_type_key,
    diagnosis_count,
    procedure_count,
    total_allowed_amount,
    total_claim_amount,
    length_of_stay_days,
    is_readmission_candidate
)
SELECT
    e.encounter_id,

    DATE_FORMAT(e.encounter_date, '%Y%m%d') AS date_key,
    DATE_FORMAT(e.discharge_date, '%Y%m%d') AS discharge_date_key,

    dp.patient_key,
    dprov.provider_key,
    ds.specialty_key,
    dd.department_key,
    det.encounter_type_key,

    COUNT(DISTINCT ed.diagnosis_id) AS diagnosis_count,
    COUNT(DISTINCT ep.procedure_id) AS procedure_count,

    SUM(b.allowed_amount) AS total_allowed_amount,
    SUM(b.claim_amount) AS total_claim_amount,

    DATEDIFF(e.discharge_date, e.encounter_date) AS length_of_stay_days,

    CASE WHEN e.encounter_type = 'Inpatient' THEN TRUE ELSE FALSE END

FROM production.encounters e

JOIN star_schema.dim_patient dp
    ON e.patient_id = dp.patient_id
JOIN star_schema.dim_provider dprov
    ON e.provider_id = dprov.provider_id
JOIN star_schema.dim_specialty ds
    ON dprov.specialty_key = ds.specialty_key
JOIN star_schema.dim_department dd
    ON e.department_id = dd.department_id
JOIN star_schema.dim_encounter_type det
    ON e.encounter_type = det.encounter_type_code

LEFT JOIN production.encounter_diagnoses ed
    ON e.encounter_id = ed.encounter_id
LEFT JOIN production.encounter_procedures ep
    ON e.encounter_id = ep.encounter_id
LEFT JOIN production.billing b
    ON e.encounter_id = b.encounter_id

GROUP BY e.encounter_id
ON DUPLICATE KEY UPDATE 
    diagnosis_count = VALUES(diagnosis_count),
    procedure_count = VALUES(procedure_count),
    total_allowed_amount = VALUES(total_allowed_amount),
    total_claim_amount = VALUES(total_claim_amount),
    length_of_stay_days = VALUES(length_of_stay_days);


#10 BRIDGE: ENCOUNTER ↔ DIAGNOSIS


INSERT INTO star_schema.bridge_encounter_diagnoses (
    encounter_key,
    diagnosis_key,
    diagnosis_sequence,
    is_primary_diagnosis
)
SELECT
    fe.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence,
    CASE WHEN ed.diagnosis_sequence = 1 THEN TRUE ELSE FALSE END
FROM production.encounter_diagnoses ed
JOIN star_schema.fact_encounters fe
    ON ed.encounter_id = fe.encounter_id
JOIN star_schema.dim_diagnosis dd
    ON ed.diagnosis_id = dd.diagnosis_id
ON DUPLICATE KEY UPDATE diagnosis_sequence = VALUES(diagnosis_sequence);


# 1️1 BRIDGE: ENCOUNTER ↔ PROCEDURE


INSERT INTO star_schema.bridge_encounter_procedures (
    encounter_key,
    procedure_key,
    procedure_date_key
)
SELECT
    fe.encounter_key,
    dp.procedure_key,
    DATE_FORMAT(ep.procedure_date, '%Y%m%d')
FROM production.encounter_procedures ep
JOIN star_schema.fact_encounters fe
    ON ep.encounter_id = fe.encounter_id
JOIN star_schema.dim_procedure dp
    ON ep.procedure_id = dp.procedure_id
ON DUPLICATE KEY UPDATE procedure_date_key = VALUES(procedure_date_key);

