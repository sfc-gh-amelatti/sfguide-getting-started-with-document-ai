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

-- ALTER STAGE doc_ai_stage REFRESH;
-- SELECT * FROM INVOICE_STREAM;
-- SELECT * FROM DIRECTORY(@DOC_AI_STAGE);
-- LS @doc_ai_stage;

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
        WHERE METADATA$ACTION = 'INSERT' OR METADATA$ACTION = 'UPDATE'
        );

        MERGE INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS AS target_table
        USING( 
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
            LATERAL FLATTEN(input => p.json_data:order_info) f_order
            )
        AS new_docai_items
        ON target_table.INVOICE_ID = new_docai_items.INVOICE_ID

         WHEN MATCHED THEN UPDATE SET
            target_table.invoice_id = new_docai_items.invoice_id,
            target_table.product_name = new_docai_items.product_name,   
            target_table.quantity = new_docai_items.quantity,           -- Update values in case they changed (though unlikely for auto-reconciled)
            target_table.unit_price = new_docai_items.unit_price,
            target_table.total_price = new_docai_items.total_price,
            target_table.file_name = new_docai_items.file_name,     -- Ensure review status is set
            target_table.file_size = new_docai_items.file_size, -- Update timestamp
            target_table.last_modified = new_docai_items.last_modified,
            target_table.snowflake_file_url = new_docai_items.snowflake_file_url  
        
        -- Action if the auto-reconciled item is new to the Gold table
        WHEN NOT MATCHED THEN INSERT (
          invoice_id,
          product_name,
          quantity,
          unit_price,
          total_price,
          file_name,
          file_size,
          last_modified,
          snowflake_file_url
        ) VALUES (
            new_docai_items.invoice_id,
            new_docai_items.product_name,   
            new_docai_items.quantity,    
            new_docai_items.unit_price,
            new_docai_items.total_price,
            new_docai_items.file_name,
            new_docai_items.file_size,
            new_docai_items.last_modified,
            new_docai_items.snowflake_file_url  
        );

        MERGE INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS AS target_table
        USING(
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
                doc_ai_qs_db.doc_ai_schema.docai_parsed p
            )
        AS new_docai_totals
        ON target_table.INVOICE_ID = new_docai_totals.INVOICE_ID

         WHEN MATCHED THEN UPDATE SET
            target_table.invoice_id = new_docai_totals.invoice_id,
            target_table.invoice_date = new_docai_totals.invoice_date,   
            target_table.subtotal = new_docai_totals.subtotal,           -- Update values in case they changed (though unlikely for auto-reconciled)
            target_table.tax = new_docai_totals.tax,
            target_table.total = new_docai_totals.total,
            target_table.file_name = new_docai_totals.file_name,     -- Ensure review status is set
            target_table.file_size = new_docai_totals.file_size, -- Update timestamp
            target_table.last_modified = new_docai_totals.last_modified,
            target_table.snowflake_file_url = new_docai_totals.snowflake_file_url  
        
        -- Action if the auto-reconciled item is new to the Gold table
        WHEN NOT MATCHED THEN INSERT (
          invoice_id,
          invoice_date,
          subtotal,
          tax,
          total,
          file_name,
          file_size,
          last_modified,
          snowflake_file_url
        ) VALUES (
            new_docai_totals.invoice_id,
            new_docai_totals.invoice_date,   
            new_docai_totals.subtotal,    
            new_docai_totals.tax,
            new_docai_totals.total,
            new_docai_totals.file_name,
            new_docai_totals.file_size,
            new_docai_totals.last_modified,
            new_docai_totals.snowflake_file_url  
        );    
    END;

    --     INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS
    --     SELECT
    --         -- Fields from the 'invoice_info' array (assuming only one element at index 0)
    --         -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
    --         REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1), '#', '') AS invoice_id,
    --         SPLIT_PART(f_order.value:value::STRING, '|', 1) AS product_name,
    --         TRY_CAST(SPLIT_PART(f_order.value:value::STRING, '|', 2) AS NUMBER(12, 2)) AS quantity,
    --         -- Attempt to cast amount_price to DECIMAL, removing commas and handling potential errors
    --         TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 3), '$', ''), ',', '') AS NUMBER(12, 2)) AS unit_price,
    --         TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 4), '$', ''), ',', '') AS NUMBER(12, 2)) AS total_price,
    --         -- f_order.value:score::FLOAT AS item_ocr_score,
    --         file_name,
    --         file_size,
    --         last_modified,
    --         snowflake_file_url
    --     FROM
    --         doc_ai_qs_db.doc_ai_schema.docai_parsed p,
    --         -- Use LATERAL FLATTEN to create a new row for each item in the 'order_info' array
    --         LATERAL FLATTEN(input => p.json_data:order_info) f_order;

    --     INSERT INTO doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS
    --     SELECT
    --         -- Fields from the 'totals' array (assuming only one element at index 0)
    --         -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
    --         REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1), '#', '') AS invoice_id,
    --         TRY_CAST(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 2) AS DATE) AS invoice_date,
    --         TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 3), '$', ''), ',', '') AS NUMBER(12, 2)) AS subtotal,
    --         TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 4), '$', ''), ',', '') AS NUMBER(12, 2)) AS tax,
    --         TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 5), '$', ''), ',', '') AS NUMBER(12, 2)) AS total,
    --         -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS total_ocr_score,
    --         file_name,
    --         file_size,
    --         last_modified,
    --         snowflake_file_url
    --     FROM
    --         doc_ai_qs_db.doc_ai_schema.docai_parsed p;
    
    -- END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT RESUME;

--ALTER TASK doc_ai_qs_db.doc_ai_schema.DOCAI_EXTRACT SUSPEND;

-- SCHEMA FOR THE STREAMLIT APP
CREATE SCHEMA doc_ai_qs_db.streamlit_schema;

-- TABLE FOR THE STREAMLIT APP
-- CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_INVOICES_VERIFIED
-- (
--     file_name string
--     , snowflake_file_url string
--     , verification_date TIMESTAMP
--     , verification_user string
-- );

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (
    invoice_id VARCHAR(255),
    product_name VARCHAR(255),
    quantity NUMBER(10, 2),
    unit_price NUMBER(10, 2),
    total_price NUMBER(12, 2)
);


 CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS (
    invoice_id VARCHAR(255),
    invoice_date DATE,
    subtotal NUMBER(10, 2),
    tax NUMBER(10, 2),
    total NUMBER(12, 2)
);

SELECT * FROM TRANSACT_TOTALS;
SELECT * FROM DOCAI_INVOICE_ITEMS;
SELECT * FROM DOCAI_INVOICE_TOTALS;
SELECT * FROM BRONZE_DB_STREAM;

SHOW STREAMS;
SHOW TASKS;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('hour',-1,current_timestamp()),
    RESULT_LIMIT => 10,
    TASK_NAME=>'RECONCILE'));
-- It might be more efficient to have a single GOLD_INVOICES table combining items and totals,
-- but separate tables often align better with source structures. Adjust as needed.