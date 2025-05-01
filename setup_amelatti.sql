USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE doc_ai_qs_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_qs_role;
GRANT EXECUTE TASK on account to role doc_ai_qs_role;

GRANT ROLE doc_ai_qs_role TO USER AMELATTI;

-- CREATE A WAREHOUSE TO BE USED
CREATE WAREHOUSE doc_ai_qs_wh;

-- GIVE THE doc_ai_qs_role ROLE ACCESS TO THE WAREHOUSE
GRANT USAGE, OPERATE, MODIFY ON WAREHOUSE doc_ai_qs_wh TO ROLE doc_ai_qs_role;

-- CREATE DATABASE AND SCHEMA TO BE USED, GIVE THE doc_ai_qs_role ACCESS
CREATE DATABASE doc_ai_qs_db;
GRANT CREATE SCHEMA, MODIFY, USAGE ON DATABASE doc_ai_qs_db TO ROLE doc_ai_qs_role;

-- CHANGE TO THE QUICKSTART ROLE
USE ROLE doc_ai_qs_role;

-- CREATE A SCHEMA FOR THE DOCUEMNT AI MODEL, STAGE etc
CREATE SCHEMA doc_ai_qs_db.doc_ai_schema;
-- EXPLICIT GRANT USAGE AND snowflake.ml.document_intelligence on the  SCHEMA
GRANT USAGE ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE table ON SCHEMA doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE snowflake.ml.document_intelligence on schema doc_ai_qs_db.doc_ai_schema to role doc_ai_qs_role;
GRANT CREATE MODEL ON SCHEMA doc_ai_qs_db.doc_ai_schema TO ROLE doc_ai_qs_role;

-- CREATE A STAGE FOR STORING DOCUMENTS
CREATE OR REPLACE STAGE doc_ai_qs_db.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');


USE SCHEMA doc_ai_qs_db.doc_ai_schema;

-- CREATE A STREAM TO MONITOR THE STAGE FOR NEW FILE UPLOADS
CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM 
ON DIRECTORY(@DOC_AI_STAGE);

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS (
    invoice_id VARCHAR(255),
    product_name VARCHAR(255),
    quantity NUMBER(12, 2),
    unit_price NUMBER(12, 2),
    total_price NUMBER(12, 2),
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255)
);

 CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS (
    invoice_id VARCHAR(255),
    invoice_date DATE,
    subtotal NUMBER(12, 2),
    tax NUMBER(12, 2),
    total NUMBER(12, 2),
    file_name VARCHAR(255),
    file_size NUMBER(12, 2),
    last_modified TIMESTAMP_TZ,
    snowflake_file_url VARCHAR(255)
);

-- CREATE A TASK TO RUN WHEN THE STREAM DETECTS NEW FILE UPLOADS IN OUR STAGE
create or replace task doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT
	warehouse=doc_ai_qs_wh
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('INVOICE_STREAM')
	as BEGIN
        CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.docai_parsed AS (
        SELECT
            Relative_path as file_name,
            size as file_size,
            last_modified,
            file_url as snowflake_file_url,
            PARSE_JSON(DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_QS_INVOICES!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1)) AS json_data
        FROM doc_ai_qs_db.doc_ai_schema.INVOICE_STREAM
        WHERE METADATA$ACTION = 'INSERT'
        );

        INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS
        SELECT
            -- Fields from the 'invoice_info' array (assuming only one element at index 0)
            -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
            REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1), '#', '') AS invoice_id,
            SPLIT_PART(f_order.value:value::STRING, '|', 1) AS product_name,
            TRY_CAST(SPLIT_PART(f_order.value:value::STRING, '|', 2) AS NUMBER(12, 2)) AS quantity,
            -- Attempt to cast amount_price to DECIMAL, removing commas and handling potential errors
            TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 3), '$', ''), ',', '') AS NUMBER(12, 2)) AS unit_price,
            TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 4), '$', ''), ',', '') AS NUMBER(12, 2)) AS total_price,
            -- f_order.value:score::FLOAT AS item_ocr_score,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url
        FROM
            doc_ai_qs_db.doc_ai_schema.docai_parsed p,
            -- Use LATERAL FLATTEN to create a new row for each item in the 'order_info' array
            LATERAL FLATTEN(input => p.json_data:order_info) f_order;

        INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS
        SELECT
            -- Fields from the 'totals' array (assuming only one element at index 0)
            -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
            REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1), '#', '') AS invoice_id,
            TRY_CAST(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 2) AS DATE) AS invoice_date,
            TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 3), '$', ''), ',', '') AS NUMBER(12, 2)) AS subtotal,
            TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 4), '$', ''), ',', '') AS NUMBER(12, 2)) AS tax,
            TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 5), '$', ''), ',', '') AS NUMBER(12, 2)) AS total,
            -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS total_ocr_score,
            file_name,
            file_size,
            last_modified,
            snowflake_file_url
        FROM
            doc_ai_qs_db.doc_ai_schema.docai_parsed p;
    
    END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT RESUME;

--ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT SUSPEND;

-- SCHEMA FOR THE STREAMLIT APP
CREATE SCHEMA doc_ai_qs_db.streamlit_schema;

-- TABLE FOR THE STREAMLIT APP
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_INVOICES_VERIFIED
(
    file_name string
    , snowflake_file_url string
    , verification_date TIMESTAMP
    , verification_user string
);

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (
    invoice_id VARCHAR(255),
    product_name VARCHAR(255),
    quantity NUMBER(10, 2),
    unit_price NUMBER(10, 2),
    total_price NUMBER(12, 2)
);

-- Example INSERT statement for the first few rows
INSERT INTO doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (invoice_id, product_name, quantity, unit_price, total_price) VALUES
  ('6', 'Apples (kg)', 2, 16.82, 33.64),
  ('6', 'Rice (kg)', 1, 8.65, 8.65),
  ('6', 'Apples (kg)', 2, 12.43, 24.86),
  ('6', 'Tomatoes (kg)', 4, 19.13, 19.13), -- Intentionally change the quantity from 1 -> 4
  ('6', 'Bread (loaf)', 4, 1.04, 4.16),
  ('9', 'Bread (loaf)', 5, 13.86, 69.3),
  ('9', 'Eggs (dozen)', 3, 15.26, 45.78),
  ('9', 'Bananas (kg)', 2, 2.7, 5.4),
  ('9', 'Milk (ltr)', 2, 1.73, 3.46),
  ('9', 'Eggs (dozen)', 2, 11.31, 22.62),
  ('8', 'Bread (loaf)', 4, 18.93, 75.72),
  ('8', 'Bread (loaf)', 3, 12.98, 38.94),
  ('8', 'Potatoes (kg)', 1, 12.97, 12.97),
  ('8', 'Cheese (pack)', 4, 5.19, 20.76),
  ('8', 'Milk (ltr)', 1, 1.23, 1.23),
  ('8', 'Milk (ltr)', 5, 18.5, 92.5),
  ('8', 'Chicken (kg)', 4, 7.99, 28.01), -- Intentionally change the price from 31.96 -> 28.01
  ('7', 'Apples (kg)', 3, 16.82, 50.46),
  ('7', 'Eggs (dozen)', 2, 9.67, 19.34),
  ('7', 'Apples (kg)', 4, 8.53, 34.12),
  ('7', 'Eggs (dozen)', 1, 14.45, 14.45),
  ('7', 'Bananas (kg)', 4, 10.23, 40.92),
  ('5', 'Apples (kg)', 5, 1.78, 8.9),
  ('5', 'Eggs (dozen)', 1, 5.61, 5.61),
  ('5', 'Milk (ltr)', 2, 4.69, 9.38),
  ('5', 'Cheese (pack)', 3, 7.82, 23.46),
  ('5', 'Milk (ltr)', 3, 15.0, 45.0),
  ('5', 'Tomatoes (kg)', 1, 18.01, 18.01),
  ('4', 'Tomatoes (kg)', 5, 9.59, 47.95),
  ('4', 'Apples (kg)', 4, 1.15, 4.6),
  ('4', 'Milk (ltr)', 3, 18.4, 55.2),
  ('4', 'Apples (kg)', 2, 4.38, 8.76),
  ('4', 'Eggs (dozen)', 3, 1.24, 3.72),
  ('3', 'Chicken (kg)', 1, 7.22, 7.22),
  ('3', 'Potatoes (kg)', 4, 6.35, 25.4),
  ('3', 'Apples (kg)', 3, 17.22, 51.66),
  ('3', 'Potatoes (kg)', 3, 12.06, 36.18),
  ('3', 'Bananas (kg)', 4, 3.55, 14.2),
  ('3', 'Tomatoes (kg)', 1, 10.56, 10.56),
  ('1', 'Bread (loaf)', 2, 16.16, 32.32),
  ('1', 'Milk (gal)', 1, 16.53, 16.53), -- Intentionally change the name from Milk (ltr) -> Milk (gal)
  ('1', 'Bread (loaf)', 4, 12.09, 48.36),
  ('1', 'Tomatoes (kg)', 2, 8.45, 16.9),
  ('1', 'Milk (ltr)', 1, 10.58, 10.58),
  ('1', 'Eggs (dozen)', 2, 1.76, 3.52),
  ('1', 'Chicken (kg)', 3, 17.5, 52.5),
  ('10', 'Bread (loaf)', 4, 12.32, 49.28),
  ('10', 'Cheese (pack)', 3, 16.6, 49.8),
  ('10', 'Chicken (kg)', 4.5, 14.75, 59.0), -- Intentionally change the quantity from 4 -> 4.5
  ('10', 'Cheese (pack)', 2, 3.72, 7.44), 
  ('10', 'Rice (kg)', 3, 4.75, 14.25),
  ('10', 'Potatoes (kg)', 5, 1.13, 5.65),
  ('10', 'Potatoes (kg)', 2, 12.36, 24.72),
  ('10', 'Rice (kg)', 5, 17.54, 87.7);
    
 SELECT * FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS;

 CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS (
    invoice_id VARCHAR(255),
    invoice_date DATE,
    subtotal NUMBER(10, 2),
    tax NUMBER(10, 2),
    total NUMBER(12, 2)
);

-- Example INSERT statement for the first few rows
INSERT INTO doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS (invoice_id, invoice_date, subtotal, tax, total) VALUES
  ('6', '2025-04-09', 90.44, 9.04, 99.48),
  ('9', '2025-02-24', 146.56, 14.66, 161.22),
  ('8', '2025-04-25', 274.08, 27.41, 301.49),
  ('7', '2025-02-04', 159.29, 15.93, 175.22),
  ('5', '2025-03-10', 110.36, 11.04, 121.40),
  ('4', '2025-01-13', 120.23, 12.02, 155.23), -- Intentionally change the grand total from 132.25 -> 155.23
  ('3', '2025-01-12', 145.22, 20.55, 159.74), -- Intentionally change the tax from 14.52 -> 20.55
  ('1', '2025-04-16', 180.71, 18.07, 198.78),
  ('10', '2025-04-01', 297.84, 29.78, 327.62);
    
 SELECT * FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS;

 -- Gold table for corrected transaction items
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS (
    invoice_id VARCHAR,
    product_name VARCHAR,
    quantity NUMBER, -- Assuming numeric type
    unit_price DECIMAL(10,2), -- Assuming decimal for currency
    total_price DECIMAL(10,2), -- Assuming decimal for currency
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    notes VARCHAR, -- Link to notes in silver or separate notes field
    -- line_instance_number NUMBER
);

-- Gold table for corrected transaction totals
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS (
    invoice_id VARCHAR,
    invoice_date DATE, -- Assuming DATE type
    subtotal DECIMAL(10,2), -- Assuming decimal for currency
    tax DECIMAL(10,2), -- Assuming decimal for currency
    total DECIMAL(10,2), -- Assuming decimal for currency
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    -- original_docai_subtotal DECIMAL(10,2), -- Optional
    -- original_docai_tax DECIMAL(10,2),      -- Optional
    -- original_docai_total DECIMAL(10,2),    -- Optional
    notes VARCHAR, -- Link to notes in silver or separate notes field
);

-- It might be more efficient to have a single GOLD_INVOICES table combining items and totals,
-- but separate tables often align better with source structures. Adjust as needed.