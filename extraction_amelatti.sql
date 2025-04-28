USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

LS @doc_ai_stage;

----

-- CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS
-- AS
-- WITH parsed_json AS (
--     SELECT
--         Relative_path as file_name,
--         size as file_size,
--         last_modified,
--         file_url as snowflake_file_url,
--         PARSE_JSON(DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_QS_CO_INVOICES_2!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1)) AS json_data
--     from directory(@doc_ai_stage)
--     -- Optional: Add a WHERE clause here if you need to filter specific rows
--     -- WHERE your_condition_column = 'some_value'
-- )

-- -- Final SELECT statement to extract and flatten the data
-- SELECT
--     file_name,
--     file_size,
--     last_modified,
--     snowflake_file_url,
--     p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score,
--     -- Fields from the 'order_info' array elements (one row per element)
--     SPLIT_PART(f_order.value:value::STRING, '|', 1) AS product_id,
--     SPLIT_PART(f_order.value:value::STRING, '|', 2) AS product_name,
--     -- Attempt to cast quantity to INTEGER, handling potential errors
--     TRY_CAST(SPLIT_PART(f_order.value:value::STRING, '|', 3) AS INTEGER) AS quantity,
--     -- Attempt to cast amount_price to DECIMAL, removing commas and handling potential errors
--     TRY_CAST(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 4), ',', '') AS DECIMAL(18, 2)) AS piece_cost,
--     TRY_CAST(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 5), ',', '') AS DECIMAL(18, 2)) AS item_cost,
--     f_order.value:score::FLOAT AS item_ocr_score,

--     -- Fields from the 'total_info' array (assuming only one element at index 0)
--     -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
--     TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1), '$', ''), ',', '') AS DATE) AS invoice_date,
--     TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 2), '$', ''), ',', '') AS DECIMAL(18, 2)) AS subtotal,
--     TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 3), '$', ''), ',', '') AS DECIMAL(18, 2)) AS tax,
--     TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 4), '$', ''), ',', '') AS DECIMAL(18, 2)) AS total,

--     -- Field from the '__documentMetadata' object
--     -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score

-- FROM
--     parsed_json p,
--     -- Use LATERAL FLATTEN to create a new row for each item in the 'order_info' array
--     LATERAL FLATTEN(input => p.json_data:order_info) f_order;

CREATE OR REPLACE TEMPORARY TABLE parsed_json AS (
    SELECT
        Relative_path as file_name,
        size as file_size,
        last_modified,
        file_url as snowflake_file_url,
        PARSE_JSON(DOC_AI_QS_DB.DOC_AI_SCHEMA.DOC_AI_QS_CO_INVOICES_2!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 2)) AS json_data
    from directory(@doc_ai_stage)
    -- Optional: Add a WHERE clause here if you need to filter specific rows
    -- WHERE your_condition_column = 'some_value'
);

SELECT * FROM parsed_json;

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_INVOICES_ITEMS
AS
-- Final SELECT statement to extract and flatten the data
SELECT
    -- Fields from the 'total_info' array (assuming only one element at index 0)
    -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
    SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1) AS invoice_id,
    -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score,
    -- Fields from the 'order_info' array elements (one row per element)
    SPLIT_PART(f_order.value:value::STRING, '|', 1) AS product_id,
    SPLIT_PART(f_order.value:value::STRING, '|', 2) AS product_name,
    -- Attempt to cast quantity to INTEGER, handling potential errors
    TRY_CAST(SPLIT_PART(f_order.value:value::STRING, '|', 3) AS INTEGER) AS quantity,
    -- Attempt to cast amount_price to DECIMAL, removing commas and handling potential errors
    TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 4), '$', ''), ',', '') AS DECIMAL(18, 2)) AS unit_price,
    TRY_CAST(REPLACE(REPLACE(SPLIT_PART(f_order.value:value::STRING, '|', 5), '$', ''), ',', '') AS DECIMAL(18, 2)) AS total_price,
    f_order.value:score::FLOAT AS item_ocr_score,
    file_name,
    file_size,
    last_modified,
    snowflake_file_url,

    -- Field from the '__documentMetadata' object
    -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score

FROM
    parsed_json p,
    -- Use LATERAL FLATTEN to create a new row for each item in the 'order_info' array
    LATERAL FLATTEN(input => p.json_data:order_info) f_order;


SELECT * FROM doc_ai_qs_db.doc_ai_schema.CO_INVOICES_ITEMS;

CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS
AS
-- Final SELECT statement to extract and flatten the data
SELECT
    -- Fields from the 'total_info' array (assuming only one element at index 0)
    -- Remove '$' and ',' characters, then attempt to cast to DECIMAL
    SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 1) AS invoice_id,
    TRY_CAST(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 2) AS DATE) AS invoice_date,
    TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 3), '$', ''), ',', '') AS DECIMAL(18, 2)) AS subtotal,
    TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 4), '$', ''), ',', '') AS DECIMAL(18, 2)) AS tax,
    TRY_CAST(REPLACE(REPLACE(SPLIT_PART(p.json_data:total_info[0]:value::STRING, '|', 5), '$', ''), ',', '') AS DECIMAL(18, 2)) AS total,
    p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score,
    file_name,
    file_size,
    last_modified,
    snowflake_file_url,

    -- Field from the '__documentMetadata' object
    -- p.json_data:"__documentMetadata":ocrScore::FLOAT AS ocr_score

FROM
    parsed_json p;


SELECT * FROM doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS;