-- STAR SCHEMA: DIMENSION TABLES

CREATE DATABASE IF NOT EXISTS star_schema;
USE star_schema;


-- Date Dimension

-- Purpose:
-- Central time dimension used for all date-based analytics.
-- Enables slicing facts by day, month, quarter, year, and weekend.
-- Reused by multiple date fields (admission date, discharge date,
-- procedure date) to ensure consistent time analysis.
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,              -- Surrogate key in YYYYMMDD format 
    calendar_date DATE NOT NULL,           -- Actual calendar date
    day_of_month TINYINT,
    month TINYINT,
    month_name VARCHAR(20),
    quarter TINYINT,
    year SMALLINT,
    is_weekend BOOLEAN
);


-- Patient Dimension
-- Purpose:
-- Stores descriptive attributes about patients.
-- Used to analyze encounters by demographics such as gender,
-- age, age group, and patient history.
-- Contains a surrogate key for OLAP and retains the OLTP patient_id
-- as a natural key for traceability.
CREATE TABLE dim_patient (
    patient_key INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT NOT NULL,                -- OLTP natural key
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    age INT,
    age_group VARCHAR(20),
    mrn VARCHAR(20),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    INDEX (patient_id)
);


-- Specialty Dimension
-- Purpose:
-- Represents the medical specialty involved in an encounter
-- (e.g., Cardiology, Pediatrics).
-- Enables specialty-level analysis such as workload, revenue,
-- and outcome comparisons across specialties.
CREATE TABLE dim_specialty (
    specialty_key INT AUTO_INCREMENT PRIMARY KEY,
    specialty_id INT NOT NULL,               -- OLTP natural key
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    UNIQUE (specialty_id)
);


-- Department Dimension
-- Purpose:
-- Describes the physical or organizational department where
-- encounters take place.
-- Supports operational analytics like capacity utilization,
-- department-level volume, and performance analysis.
CREATE TABLE dim_department (
    department_key INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT NOT NULL,              -- OLTP natural key
    department_name VARCHAR(100),
    floor INT,
    capacity INT,
    UNIQUE (department_id)
);


-- Provider Dimension
-- Purpose:
-- Stores information about healthcare providers (doctors, clinicians).
-- Linked to specialty and department dimensions.
-- Enables provider-level analytics such as productivity,
-- case mix, and performance metrics.
CREATE TABLE dim_provider (
    provider_key INT AUTO_INCREMENT PRIMARY KEY,
    provider_id INT NOT NULL,                -- OLTP natural key
    provider_name VARCHAR(200),
    credential VARCHAR(20),
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    INDEX (provider_id)
);


-- Encounter Type Dimension
-- Purpose:
-- Classifies encounters by type (Inpatient, Outpatient, ER).
-- Allows filtering and comparison of utilization patterns
-- and outcomes across encounter categories.
CREATE TABLE dim_encounter_type (
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY,
    encounter_type_code VARCHAR(50),         -- Inpatient, Outpatient, ER
    encounter_type_description VARCHAR(100),
    UNIQUE (encounter_type_code)
);


-- OPTIONAL: Diagnosis Dimension

-- Purpose:
-- Stores standardized diagnosis information (ICD-10).
-- Kept separate from the fact table to avoid fact table explosion
-- because an encounter can have multiple diagnoses.
CREATE TABLE dim_diagnosis (
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY,
    diagnosis_id INT NOT NULL,               -- OLTP natural key
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200),
    UNIQUE (diagnosis_id)
);


-- OPTIONAL: Procedure Dimension
-- Purpose:
-- Stores standardized procedure information (CPT codes).
-- Used for procedure-level analytics without inflating the fact table.
CREATE TABLE dim_procedure (
    procedure_key INT AUTO_INCREMENT PRIMARY KEY,
    procedure_id INT NOT NULL,               -- OLTP natural key
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200),
    UNIQUE (procedure_id)
);



-- FACT TABLE: Encounter-Level Facts
-- Purpose:
-- Central fact table that records measurable data about encounters.
-- Grain: ONE ROW PER ENCOUNTER.
-- Contains foreign keys to all core dimensions and
-- pre-aggregated metrics for fast analytical queries.
CREATE TABLE fact_encounters (
    encounter_key BIGINT AUTO_INCREMENT PRIMARY KEY,

    
    -- Foreign Keys (Dimensions)
    encounter_id INT NOT NULL,                    -- OLTP natural key (degenerate)
    date_key INT NOT NULL,                        -- Encounter start date
    discharge_date_key INT,                       -- Discharge date (if applicable)
    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    specialty_key INT NOT NULL,
    department_key INT NOT NULL,
    encounter_type_key INT NOT NULL,

    
    -- Pre-Aggregated Metrics
   
    diagnosis_count INT DEFAULT 0,
    procedure_count INT DEFAULT 0,
    total_allowed_amount DECIMAL(12,2) DEFAULT 0.00,
    total_claim_amount DECIMAL(12,2) DEFAULT 0.00,
    length_of_stay_days INT,
    is_readmission_candidate BOOLEAN,

    -- Constraints
   
    UNIQUE (encounter_id),

    FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date (date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient (patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider (provider_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty (specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department (department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type (encounter_type_key)
);


-- Indexes for Analytical Queries

-- Time-based analytics
CREATE INDEX idx_fact_date
    ON fact_encounters (date_key);

CREATE INDEX idx_fact_specialty_date
    ON fact_encounters (specialty_key, date_key);

-- Readmission analysis
CREATE INDEX idx_fact_patient_date
    ON fact_encounters (patient_key, date_key);

-- Revenue analysis
CREATE INDEX idx_fact_specialty_revenue
    ON fact_encounters (specialty_key, total_allowed_amount);

-- Encounter type filtering
CREATE INDEX idx_fact_encounter_type
    ON fact_encounters (encounter_type_key);



-- BRIDGE TABLES: Many-to-Many Relationships


-- Bridge: Encounter ↔ Diagnosis

-- Purpose:
-- Resolves the many-to-many relationship between encounters and diagnoses.
-- Preserves full diagnostic detail without duplicating fact rows.
-- Supports diagnosis sequencing and primary diagnosis identification.
CREATE TABLE bridge_encounter_diagnoses (
    encounter_key BIGINT NOT NULL,
    diagnosis_key INT NOT NULL,

    -- Clinical context
    diagnosis_sequence INT,
    is_primary_diagnosis BOOLEAN,

    -- Constraints
    PRIMARY KEY (encounter_key, diagnosis_key),

    FOREIGN KEY (encounter_key)
        REFERENCES fact_encounters (encounter_key),
    FOREIGN KEY (diagnosis_key)
        REFERENCES dim_diagnosis (diagnosis_key)
);

-- Index to support diagnosis-driven analytics
CREATE INDEX idx_bridge_diag_diagnosis
    ON bridge_encounter_diagnoses (diagnosis_key);



-- Bridge: Encounter ↔ Procedure

-- Purpose:
-- Resolves the many-to-many relationship between encounters and procedures.
-- Allows procedure-level analysis and timing without increasing
-- the grain of the fact table.
CREATE TABLE bridge_encounter_procedures (
    encounter_key BIGINT NOT NULL,
    procedure_key INT NOT NULL,

    -- Procedure timing
    procedure_date_key INT,

    -- Constraints
    PRIMARY KEY (encounter_key, procedure_key),

    FOREIGN KEY (encounter_key)
        REFERENCES fact_encounters (encounter_key),
    FOREIGN KEY (procedure_key)
        REFERENCES dim_procedure (procedure_key),
    FOREIGN KEY (procedure_date_key)
        REFERENCES dim_date (date_key)
);

-- Index to support procedure-driven analytics
CREATE INDEX idx_bridge_proc_procedure
    ON bridge_encounter_procedures (procedure_key);




-- 1) Populate dimension tables from production

-- dim_date
INSERT INTO star_schema.dim_date (
    date_key, calendar_date, day_of_month, month, month_name, quarter, year, is_weekend
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

-- dim_specialty
INSERT INTO star_schema.dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code
FROM production.specialties
ON DUPLICATE KEY UPDATE 
    specialty_name = VALUES(specialty_name),
    specialty_code = VALUES(specialty_code);

-- dim_department
INSERT INTO star_schema.dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity
FROM production.departments
ON DUPLICATE KEY UPDATE 
    department_name = VALUES(department_name),
    floor = VALUES(floor),
    capacity = VALUES(capacity);

-- dim_patient (SCD Type 2)
-- 1. Expire changed records
UPDATE star_schema.dim_patient dp
JOIN production.patients p ON dp.patient_id = p.patient_id
SET dp.effective_end_date = CURDATE(),
    dp.is_current = FALSE
WHERE dp.is_current = TRUE
  AND (dp.first_name <> p.first_name 
       OR dp.last_name <> p.last_name 
       OR dp.gender <> p.gender 
       OR dp.mrn <> p.mrn);

-- 2. Insert new records (both brand new and updated versions)
INSERT INTO star_schema.dim_patient (
    patient_id, first_name, last_name, gender, date_of_birth, 
    age, age_group, mrn, effective_start_date, effective_end_date, is_current
)
SELECT 
    p.patient_id, p.first_name, p.last_name, p.gender, p.date_of_birth,
    TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) AS age,
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN 'Child'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 18 AND 39 THEN 'Adult'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) BETWEEN 40 AND 64 THEN 'Middle Age'
        ELSE 'Senior'
    END AS age_group,
    p.mrn,
    CURDATE(),
    '9999-12-31',
    TRUE
FROM production.patients p
LEFT JOIN star_schema.dim_patient dp ON p.patient_id = dp.patient_id AND dp.is_current = TRUE
WHERE dp.patient_id IS NULL;

-- dim_provider (SCD Type 2)
-- 1. Expire changed records
UPDATE star_schema.dim_provider dprov
JOIN production.providers p ON dprov.provider_id = p.provider_id
SET dprov.effective_end_date = CURDATE(),
    dprov.is_current = FALSE
WHERE dprov.is_current = TRUE
  AND (dprov.provider_name <> CONCAT(p.first_name, ' ', p.last_name) 
       OR dprov.credential <> p.credential);

-- 2. Insert new records
INSERT INTO star_schema.dim_provider (
    provider_id, provider_name, credential, 
    effective_start_date, effective_end_date, is_current
)
SELECT 
    p.provider_id, 
    CONCAT(p.first_name, ' ', p.last_name) AS provider_name, 
    p.credential,
    CURDATE(),
    '9999-12-31',
    TRUE
FROM production.providers p
LEFT JOIN star_schema.dim_provider dprov ON p.provider_id = dprov.provider_id AND dprov.is_current = TRUE
WHERE dprov.provider_id IS NULL;

-- dim_encounter_type
INSERT INTO star_schema.dim_encounter_type (encounter_type_code, encounter_type_description)
SELECT DISTINCT encounter_type, CONCAT(encounter_type, ' Encounter')
FROM production.encounters
ON DUPLICATE KEY UPDATE encounter_type_description = VALUES(encounter_type_description);

-- dim_diagnosis
INSERT INTO star_schema.dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description
FROM production.diagnoses
ON DUPLICATE KEY UPDATE 
    icd10_code = VALUES(icd10_code),
    icd10_description = VALUES(icd10_description);

-- dim_procedure
INSERT INTO star_schema.dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description
FROM production.procedures
ON DUPLICATE KEY UPDATE 
    cpt_code = VALUES(cpt_code),
    cpt_description = VALUES(cpt_description);

-- 2) Populate Fact Table
INSERT INTO star_schema.fact_encounters (
    encounter_id, date_key, discharge_date_key, patient_key, provider_key,
    specialty_key, department_key, encounter_type_key,
    diagnosis_count, procedure_count, total_allowed_amount, total_claim_amount,
    length_of_stay_days, is_readmission_candidate
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
    CASE WHEN e.encounter_type = 'Inpatient' THEN TRUE ELSE FALSE END AS is_readmission_candidate
FROM production.encounters e
JOIN star_schema.dim_patient dp 
    ON e.patient_id = dp.patient_id 
    AND e.encounter_date BETWEEN dp.effective_start_date AND dp.effective_end_date
JOIN star_schema.dim_provider dprov 
    ON e.provider_id = dprov.provider_id 
    AND e.encounter_date BETWEEN dprov.effective_start_date AND dprov.effective_end_date
JOIN production.providers p_prod ON e.provider_id = p_prod.provider_id
JOIN star_schema.dim_specialty ds ON p_prod.specialty_id = ds.specialty_id
JOIN star_schema.dim_department dd ON e.department_id = dd.department_id
JOIN star_schema.dim_encounter_type det ON e.encounter_type = det.encounter_type_code
LEFT JOIN production.encounter_diagnoses ed ON e.encounter_id = ed.encounter_id
LEFT JOIN production.encounter_procedures ep ON e.encounter_id = ep.encounter_id
LEFT JOIN production.billing b ON e.encounter_id = b.encounter_id
GROUP BY e.encounter_id
ON DUPLICATE KEY UPDATE 
    diagnosis_count = VALUES(diagnosis_count),
    procedure_count = VALUES(procedure_count),
    total_allowed_amount = VALUES(total_allowed_amount),
    total_claim_amount = VALUES(total_claim_amount),
    length_of_stay_days = VALUES(length_of_stay_days);

-- 3) Populate Bridge Tables
INSERT INTO star_schema.bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence, is_primary_diagnosis)
SELECT 
    fe.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence,
    CASE WHEN ed.diagnosis_sequence = 1 THEN TRUE ELSE FALSE END
FROM production.encounter_diagnoses ed
JOIN star_schema.fact_encounters fe ON ed.encounter_id = fe.encounter_id
JOIN star_schema.dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
ON DUPLICATE KEY UPDATE diagnosis_sequence = VALUES(diagnosis_sequence);

INSERT INTO star_schema.bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key)
SELECT 
    fe.encounter_key,
    dp.procedure_key,
    DATE_FORMAT(ep.procedure_date, '%Y%m%d')
FROM production.encounter_procedures ep
JOIN star_schema.fact_encounters fe ON ep.encounter_id = fe.encounter_id
JOIN star_schema.dim_procedure dp ON ep.procedure_id = dp.procedure_id
ON DUPLICATE KEY UPDATE procedure_date_key = VALUES(procedure_date_key);
