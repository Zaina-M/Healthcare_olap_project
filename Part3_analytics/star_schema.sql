-- =========================================
-- STAR SCHEMA OVERVIEW
-- =========================================
-- This star schema is designed for analytical (OLAP) workloads.
-- It supports encounter-level healthcare analytics such as:
-- volume trends, revenue analysis, length of stay, readmissions,
-- diagnosis and procedure analysis, and provider performance.
--
-- Grain of the fact table: ONE ROW PER ENCOUNTER

-- =========================================
-- STAR SCHEMA: DIMENSION TABLES
-- =========================================

CREATE DATABASE IF NOT EXISTS star_schema;
USE star_schema;

-- =========================================
-- Date Dimension
-- =========================================
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

-- =========================================
-- Patient Dimension
-- =========================================
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
    UNIQUE (patient_id)
);

-- =========================================
-- Specialty Dimension
-- =========================================
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

-- =========================================
-- Department Dimension
-- =========================================
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

-- =========================================
-- Provider Dimension
-- =========================================
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
    specialty_key INT,
    department_key INT,
    UNIQUE (provider_id),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty (specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department (department_key)
);

-- =========================================
-- Encounter Type Dimension
-- =========================================
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

-- =========================================
-- OPTIONAL: Diagnosis Dimension
-- =========================================
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

-- =========================================
-- OPTIONAL: Procedure Dimension
-- =========================================
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


-- =========================================
-- FACT TABLE: Encounter-Level Facts
-- =========================================
-- Purpose:
-- Central fact table that records measurable data about encounters.
-- Grain: ONE ROW PER ENCOUNTER.
-- Contains foreign keys to all core dimensions and
-- pre-aggregated metrics for fast analytical queries.
CREATE TABLE fact_encounters (
    encounter_key BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- ===============================
    -- Foreign Keys (Dimensions)
    -- ===============================
    encounter_id INT NOT NULL,                    -- OLTP natural key (degenerate)
    date_key INT NOT NULL,                        -- Encounter start date
    discharge_date_key INT,                       -- Discharge date (if applicable)
    patient_key INT NOT NULL,
    provider_key INT NOT NULL,
    specialty_key INT NOT NULL,
    department_key INT NOT NULL,
    encounter_type_key INT NOT NULL,

    -- ===============================
    -- Pre-Aggregated Metrics
    -- ===============================
    diagnosis_count INT DEFAULT 0,
    procedure_count INT DEFAULT 0,
    total_allowed_amount DECIMAL(12,2) DEFAULT 0.00,
    total_claim_amount DECIMAL(12,2) DEFAULT 0.00,
    length_of_stay_days INT,
    is_readmission_candidate BOOLEAN,

    -- ===============================
    -- Constraints
    -- ===============================
    UNIQUE (encounter_id),

    FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date (date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient (patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider (provider_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty (specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department (department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type (encounter_type_key)
);

-- =========================================
-- Indexes for Analytical Queries
-- =========================================

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


-- =========================================
-- BRIDGE TABLES: Many-to-Many Relationships
-- =========================================

-- =========================================
-- Bridge: Encounter ↔ Diagnosis
-- =========================================
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


-- =========================================
-- Bridge: Encounter ↔ Procedure
-- =========================================
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

/*
Design intent 

Many-to-many relationships are isolated from the fact table

Fact table remains one row per encounter

Bridge tables preserve full clinical detail only when needed

Composite PKs prevent duplication

Indexes support diagnosis- or procedure-driven queries

What this enables:

Diagnosis–procedure pairing without fact explosion

Accurate encounter and revenue aggregation

Optional joins instead of mandatory joins

Predictable query performance

*/


-- dim_specialty
INSERT INTO dim_specialty (specialty_name, specialty_code)
SELECT DISTINCT specialty_name, specialty_code
FROM specialties
ON DUPLICATE KEY UPDATE specialty_name = VALUES(specialty_name);

-- dim_department
INSERT INTO star_schema.dim_department (department_name, department_code)
SELECT DISTINCT department_name, CAST(department_id AS CHAR)
FROM departments
ON DUPLICATE KEY UPDATE department_name = VALUES(department_name);

-- dim_provider
INSERT INTO star_schema.dim_provider (provider_name, provider_npi, provider_code)
SELECT DISTINCT CONCAT(first_name, ' ', last_name) AS provider_name,
       CAST(npi AS CHAR) AS provider_npi,
       CAST(provider_code AS CHAR) AS provider_code
FROM providers
ON DUPLICATE KEY UPDATE provider_name = VALUES(provider_name);

-- dim_patient
INSERT INTO star_schema.dim_patient (patient_first_name, patient_last_name, patient_birth_date, patient_sex, patient_mrn)
SELECT DISTINCT first_name, last_name, birth_date, gender, patient_mrn
FROM patients
ON DUPLICATE KEY UPDATE patient_first_name = VALUES(patient_first_name);

-- dim_diagnosis
INSERT INTO star_schema.dim_diagnosis (diagnosis_code, diagnosis_description)
SELECT DISTINCT diagnosis_code, diagnosis_description
FROM diagnoses
ON DUPLICATE KEY UPDATE diagnosis_description = VALUES(diagnosis_description);

-- dim_procedure
INSERT INTO star_schema.dim_procedure (procedure_code, procedure_description)
SELECT DISTINCT procedure_code, procedure_description
FROM procedures
ON DUPLICATE KEY UPDATE procedure_description = VALUES(procedure_description);

-- dim_date (populate from encounter start_timestamp and billing_date)
INSERT INTO star_schema.dim_date (`date`, year, month, day, quarter, day_of_week, is_weekend)
SELECT d, YEAR(d), MONTH(d), DAY(d), QUARTER(d), DAYOFWEEK(d), (CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END)
FROM (
  SELECT DISTINCT DATE(start_timestamp) AS d FROM encounters
  UNION
  SELECT DISTINCT billing_date AS d FROM billing
) x
WHERE d IS NOT NULL
ON DUPLICATE KEY UPDATE `date` = VALUES(`date`);

-- 2) Fact table: fact_encounters
-- This maps encounters to dimension surrogate keys by joining via natural keys in source tables.
INSERT INTO star_schema.fact_encounters (
  encounter_id,
  patient_sk,
  provider_sk,
  department_sk,
  encounter_type,
  start_timestamp,
  end_timestamp,
  encounter_date_sk,
  total_charge,
  total_paid,
  billing_status
)
SELECT
  e.encounter_id,
  dp.patient_id AS patient_sk,
  prm.provider_id AS provider_sk,
  dd.department_id AS department_sk,
  e.encounter_type,
  e.start_timestamp,
  e.end_timestamp,
  dd_date.date_id AS encounter_date_sk,
  b.charge_amount AS total_charge,
  b.paid_amount AS total_paid,
  b.status AS billing_status
FROM encounters e
LEFT JOIN patients p ON e.patient_id = p.patient_id
LEFT JOIN star_schema.dim_patient dp ON dp.patient_mrn = p.patient_mrn
LEFT JOIN providers pr ON e.provider_id = pr.provider_id
LEFT JOIN star_schema.dim_provider prm ON prm.provider_npi = CAST(pr.npi AS CHAR)
LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN star_schema.dim_department dd ON dd.department_name = d.department_name
LEFT JOIN billing b ON b.encounter_id = e.encounter_id
LEFT JOIN star_schema.dim_date dd_date ON dd_date.`date` = DATE(e.start_timestamp)
ON DUPLICATE KEY UPDATE
  total_charge = VALUES(total_charge),
  total_paid = VALUES(total_paid),
  billing_status = VALUES(billing_status);

-- 3) Bridge tables

-- bridge_encounter_diagnoses
INSERT INTO star_schema.bridge_encounter_diagnoses (encounter_id, diagnosis_sk, diagnosis_rank)
SELECT ed.encounter_id, ddx.diagnosis_id AS diagnosis_sk, ed.rank
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN star_schema.dim_diagnosis ddx ON ddx.diagnosis_code = d.diagnosis_code
ON DUPLICATE KEY UPDATE diagnosis_rank = VALUES(diagnosis_rank);

-- bridge_encounter_procedures
INSERT INTO star_schema.bridge_encounter_procedures (encounter_id, procedure_sk, procedure_rank)
SELECT ep.encounter_id, dp2.procedure_id AS procedure_sk, ep.rank
FROM encounter_procedures ep
JOIN procedures prc ON ep.procedure_id = prc.procedure_id
JOIN star_schema.dim_procedure dp2 ON dp2.procedure_code = prc.procedure_code
ON DUPLICATE KEY UPDATE procedure_rank = VALUES(procedure_rank);
