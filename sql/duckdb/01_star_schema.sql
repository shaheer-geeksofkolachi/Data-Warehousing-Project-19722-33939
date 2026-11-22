-- =====================================================================================================
-- SDI DATA MART (DuckDB) - Sales & Demand Intelligence
-- Two fact tables sharing conformed dimensions (a small fact constellation):
--   fact_sales            transaction grain      : 1 row = 1 pharmacy / e-commerce sales transaction
--   fact_hospital_demand  periodic snapshot grain: 1 row = 1 hospital x 1 medicine x 1 month
-- Conformed dimensions: dim_date (role-playing), dim_medicine (SCD2), dim_geography, dim_channel
-- Surrogate keys are integers; key -1 = "Unknown" member (no orphan facts, ever).
-- Referential integrity is enforced by the ETL (lookup with unknown-member fallback) + reconciliation checks.
-- =====================================================================================================

CREATE SEQUENCE IF NOT EXISTS seq_medicine_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_hospital_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_pharmacy_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_geography_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_warehouse_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_channel_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_promotion_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_sales_profile_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_demand_profile_key START 1;

-- ---------------------------------------------------------------- DIM_DATE (role-playing)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key          INTEGER PRIMARY KEY,      -- yyyymmdd
    full_date         DATE NOT NULL,
    day_of_month      SMALLINT,
    day_of_week       SMALLINT,                 -- 1 = Monday
    day_name          VARCHAR,
    is_weekend        BOOLEAN,
    week_of_year      SMALLINT,
    month_num         SMALLINT,
    month_name        VARCHAR,
    month_short       VARCHAR,
    quarter_num       SMALLINT,
    quarter_label     VARCHAR,                  -- 2025-Q3
    year_num          SMALLINT,
    year_month        VARCHAR,                  -- 2025-07
    month_start_date  DATE,
    is_month_start    BOOLEAN,
    is_year_start     BOOLEAN,
    days_in_month     SMALLINT
);

-- ---------------------------------------------------------------- DIM_GEOGRAPHY (shared by pharmacy, hospital, warehouse)
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key     INTEGER PRIMARY KEY,
    city              VARCHAR,
    state_code        VARCHAR,
    state_name        VARCHAR,
    census_region     VARCHAR,                  -- Northeast / Midwest / South / West
    census_division   VARCHAR,
    country_code      VARCHAR
);

-- ---------------------------------------------------------------- DIM_MEDICINE (SCD Type 2 on commercial attributes)
CREATE TABLE IF NOT EXISTS dim_medicine (
    medicine_key                  INTEGER PRIMARY KEY,
    medicine_id                   VARCHAR NOT NULL,   -- natural / business key
    internal_sku                  VARCHAR,
    gtin14                        VARCHAR,
    generic_name                  VARCHAR,            -- Type 1
    brand_name                    VARCHAR,            -- Type 1
    disease_category              VARCHAR,
    icd10_category                VARCHAR,
    atc_code                      VARCHAR,
    therapeutic_class             VARCHAR,            -- Type 1
    dosage_form                   VARCHAR,
    strength                      VARCHAR,
    route_of_administration       VARCHAR,
    pack_size                     INTEGER,            -- Type 2
    base_unit_of_measure          VARCHAR,
    prescription_class            VARCHAR,            -- Type 2
    controlled_medicine_flag      VARCHAR,
    cold_chain_flag               VARCHAR,            -- Type 2
    storage_condition             VARCHAR,            -- derived: '2-8 C' / '15-25 C'
    storage_min_temp_c            INTEGER,
    storage_max_temp_c            INTEGER,
    max_humidity_pct              INTEGER,
    shelf_life_months             INTEGER,            -- Type 2
    min_remaining_shelf_life_days INTEGER,
    primary_supplier_id           VARCHAR,
    primary_plant_id              VARCHAR,
    manufacturing_lead_time_days  INTEGER,
    standard_batch_size_packs     INTEGER,
    safety_stock_days             INTEGER,
    reorder_review_cycle_days     INTEGER,
    unit_manufacturing_cost_usd   DOUBLE,             -- Type 2
    wholesale_price_usd           DOUBLE,             -- Type 2
    recommended_retail_price_usd  DOUBLE,             -- Type 2
    price_band                    VARCHAR,            -- derived: Low (<$5) / Mid / High (>$20)
    product_status                VARCHAR,            -- Type 2
    scd_hash                      VARCHAR,
    effective_from                DATE NOT NULL,
    effective_to                  DATE NOT NULL,
    is_current                    BOOLEAN NOT NULL,
    version_num                   INTEGER NOT NULL,
    source_batch_id               VARCHAR
);

-- ---------------------------------------------------------------- DIM_HOSPITAL (SCD Type 2 on capacity attributes)
CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_key            INTEGER PRIMARY KEY,
    hospital_id             VARCHAR NOT NULL,
    hospital_gln            VARCHAR,
    hospital_name           VARCHAR,
    hospital_type           VARCHAR,          -- Type 2
    hospital_pharmacy_id    VARCHAR,
    city                    VARCHAR,
    state_code              VARCHAR,
    geography_key           INTEGER,
    licensed_beds           INTEGER,          -- Type 2
    icu_beds                INTEGER,          -- Type 2
    avg_occupancy_rate_pct  INTEGER,          -- Type 2
    trauma_level            VARCHAR,          -- Type 2
    catchment_population    BIGINT,           -- Type 2
    bed_size_band           VARCHAR,          -- derived: <400 / 400-699 / 700+
    scd_hash                VARCHAR,
    effective_from          DATE NOT NULL,
    effective_to            DATE NOT NULL,
    is_current              BOOLEAN NOT NULL,
    version_num             INTEGER NOT NULL
);

-- ---------------------------------------------------------------- DIM_PHARMACY (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_pharmacy (
    pharmacy_key     INTEGER PRIMARY KEY,
    pharmacy_id      VARCHAR NOT NULL,
    pharmacy_gln     VARCHAR,
    pharmacy_name    VARCHAR,
    pharmacy_type    VARCHAR,
    hospital_id      VARCHAR,
    city             VARCHAR,
    state_code       VARCHAR,
    geography_key    INTEGER,
    updated_at       TIMESTAMP
);

-- ---------------------------------------------------------------- DIM_WAREHOUSE (conformed for Phase 2/3)
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_key    INTEGER PRIMARY KEY,
    warehouse_id     VARCHAR NOT NULL,
    warehouse_gln    VARCHAR,
    warehouse_name   VARCHAR,
    city             VARCHAR,
    state_code       VARCHAR,
    geography_key    INTEGER
);

-- ---------------------------------------------------------------- DIM_CHANNEL (conformed across both facts)
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key      INTEGER PRIMARY KEY,
    sales_channel    VARCHAR,            -- source value: Physical Pharmacy / Ecommerce / Hospital ERP
    pharmacy_type    VARCHAR,
    channel_group    VARCHAR,            -- Retail pharmacy / Hospital pharmacy / E-commerce / Hospital ward
    channel_family   VARCHAR,            -- Commercial sale / Institutional consumption
    source_system    VARCHAR
);

-- ---------------------------------------------------------------- DIM_PROMOTION
CREATE TABLE IF NOT EXISTS dim_promotion (
    promotion_key     INTEGER PRIMARY KEY,
    promotion_code    VARCHAR,           -- 'NO_PROMO' when none
    promotion_type    VARCHAR,           -- Loyalty / Seasonal / Online / Wellness / None
    discount_pct      DOUBLE,
    is_promoted       BOOLEAN
);

-- ---------------------------------------------------------------- JUNK DIMENSIONS
-- low-cardinality flags & indicators of a sale collapsed into one dimension
CREATE TABLE IF NOT EXISTS dim_sales_profile (
    sales_profile_key           INTEGER PRIMARY KEY,
    customer_segment            VARCHAR,
    payer_type                  VARCHAR,
    payment_method              VARCHAR,
    prescription_required_flag  VARCHAR,
    ecommerce_delivery_status   VARCHAR,
    return_flag                 VARCHAR,
    sale_status                 VARCHAR,
    date_quality_flag           VARCHAR    -- 'Normal' / 'Year-start bulk posting' (data-quality audit flag)
);
CREATE TABLE IF NOT EXISTS dim_demand_profile (
    demand_profile_key          INTEGER PRIMARY KEY,
    medication_request_status   VARCHAR,
    formulary_status            VARCHAR,
    emergency_request_flag      VARCHAR,
    stockout_flag               VARCHAR,
    unfulfilled_flag            VARCHAR
);

-- ---------------------------------------------------------------- FACT_SALES (transaction grain)
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_transaction_id     VARCHAR NOT NULL,   -- degenerate dimension (PK)
    sale_date_key            INTEGER NOT NULL,   -- role: sale date
    delivery_date_key        INTEGER,            -- role: pharmacy replenishment delivery date
    expiry_date_key          INTEGER,            -- role: batch expiry date
    medicine_key             INTEGER NOT NULL,   -- SCD2 version valid on the sale date
    pharmacy_key             INTEGER NOT NULL,
    geography_key            INTEGER NOT NULL,
    channel_key              INTEGER NOT NULL,
    promotion_key            INTEGER NOT NULL,
    sales_profile_key        INTEGER NOT NULL,
    warehouse_key            INTEGER NOT NULL,
    pharmacy_order_id        VARCHAR,            -- degenerate dimensions (lineage / traceability)
    pharmacy_delivery_id     VARCHAR,
    prescription_id          VARCHAR,
    warehouse_lot_id         VARCHAR,
    production_batch_id      VARCHAR,
    sales_timestamp_utc      TIMESTAMP,
    sale_hour                SMALLINT,
    quantity_sold_packs      INTEGER,            -- additive
    unit_retail_price_usd    DOUBLE,             -- non-additive
    gross_sales_usd          DOUBLE,             -- additive
    discount_usd             DOUBLE,             -- additive
    net_sales_usd            DOUBLE,             -- additive
    sales_tax_usd            DOUBLE,             -- additive
    cogs_usd                 DOUBLE,             -- additive
    gross_margin_usd         DOUBLE,             -- additive
    returned_qty_packs       INTEGER,            -- additive
    days_to_expiry_at_sale   INTEGER,            -- non-additive (average only)
    delivery_quantity_packs  INTEGER,            -- non-additive (repeats per delivery)
    delivery_otif_flag       VARCHAR,
    ingest_mode              VARCHAR,            -- batch (historical backfill) / stream (live)
    refresh_tag              VARCHAR,
    etl_batch_id             VARCHAR,
    etl_loaded_at            TIMESTAMP
);

-- ---------------------------------------------------------------- FACT_HOSPITAL_DEMAND (periodic snapshot, monthly)
CREATE TABLE IF NOT EXISTS fact_hospital_demand (
    hospital_demand_record_id   VARCHAR NOT NULL,   -- degenerate dimension
    demand_month_key            INTEGER NOT NULL,   -- role: demand month (first day of month)
    hospital_key                INTEGER NOT NULL,   -- SCD2 version valid in that month
    medicine_key                INTEGER NOT NULL,
    geography_key               INTEGER NOT NULL,
    channel_key                 INTEGER NOT NULL,   -- constant 'Hospital ward' (conformed with sales)
    demand_profile_key          INTEGER NOT NULL,
    estimated_disease_cases     INTEGER,            -- non-additive across medicines (repeats per disease)
    disease_related_admissions  INTEGER,            -- non-additive across medicines
    outpatient_encounters       INTEGER,            -- non-additive across medicines
    forecast_demand_packs       INTEGER,            -- additive
    requested_qty_packs         INTEGER,            -- additive
    approved_qty_packs          INTEGER,            -- additive
    received_qty_packs          INTEGER,            -- additive
    emergency_request_packs     INTEGER,            -- additive
    opening_stock_packs         INTEGER,            -- semi-additive (not across time)
    dispensed_qty_packs         INTEGER,            -- additive
    consumed_qty_packs          INTEGER,            -- additive
    wastage_qty_packs           INTEGER,            -- additive
    closing_stock_packs         INTEGER,            -- semi-additive (not across time)
    unfulfilled_demand_packs    INTEGER,            -- additive
    stockout_days               INTEGER,            -- additive (hospital-medicine-days)
    service_level_pct           DOUBLE,             -- non-additive (ratio)
    avg_lead_time_days          INTEGER,            -- non-additive
    demand_trend_index          DOUBLE,             -- non-additive
    forecast_error_packs        INTEGER,            -- derived: forecast - consumed (additive -> bias)
    abs_forecast_error_packs    INTEGER,            -- derived: |forecast - consumed| (additive -> MAPE/WAPE)
    supply_gap_packs            INTEGER,            -- derived: requested - received
    refresh_tag                 VARCHAR,
    etl_batch_id                VARCHAR,
    etl_loaded_at               TIMESTAMP
);

-- ---------------------------------------------------------------- ROLE-PLAYING VIEWS of dim_date
CREATE OR REPLACE VIEW dim_sale_date AS
    SELECT date_key AS sale_date_key, full_date AS sale_date, year_num AS sale_year, quarter_label AS sale_quarter,
           year_month AS sale_year_month, month_name AS sale_month_name, day_name AS sale_day_name,
           is_weekend AS sale_is_weekend FROM dim_date;
CREATE OR REPLACE VIEW dim_delivery_date AS
    SELECT date_key AS delivery_date_key, full_date AS delivery_date, year_month AS delivery_year_month FROM dim_date;
CREATE OR REPLACE VIEW dim_expiry_date AS
    SELECT date_key AS expiry_date_key, full_date AS expiry_date, year_month AS expiry_year_month,
           year_num AS expiry_year FROM dim_date;
CREATE OR REPLACE VIEW dim_demand_month AS
    SELECT date_key AS demand_month_key, full_date AS demand_month, year_num AS demand_year,
           quarter_label AS demand_quarter, year_month AS demand_year_month, month_name AS demand_month_name
    FROM dim_date WHERE is_month_start;
