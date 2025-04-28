-- Set the context to the correct database and schema (optional if already set in your session)
USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

-- Create the target table to store discrepancies
-- Using CREATE OR REPLACE TABLE allows rerunning the script without manual cleanup
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED (
    invoice_id VARCHAR,
    transact_product_name VARCHAR,
    co_invoice_product_name VARCHAR,
    transact_quantity NUMBER, -- Adjust datatype if needed (e.g., DECIMAL, INTEGER)
    co_invoice_quantity NUMBER, -- Adjust datatype if needed
    transact_unit_price DECIMAL(18, 2), -- Use appropriate precision and scale
    co_invoice_unit_price DECIMAL(18, 2), -- Use appropriate precision and scale
    transact_total_price DECIMAL(18, 2), -- Use appropriate precision and scale
    co_invoice_total_price DECIMAL(18, 2), -- Use appropriate precision and scale
    review_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- Timestamp for when the record was added
);

----------


-- Redefine the target table to capture specific column discrepancies
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_ITEMS (
    invoice_id VARCHAR,
    product_name VARCHAR,          -- Identifier for the line item being compared
    discrepancy_type VARCHAR,      -- 'Value Mismatch' or 'Missing Item'
    column_name VARCHAR,           -- Name of the column with the difference, or 'Entire Row' for missing items
    transact_items_value VARCHAR, -- Value from TRANSACT_ITEMS (casted to VARCHAR for consistency)
    co_invoices_items_value VARCHAR, -- Value from CO_INVOICES_ITEMS (casted to VARCHAR)
    review_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Define the base join and comparison data in a CTE
CREATE OR REPLACE TEMPORARY TABLE JoinedItems AS (
    SELECT
        COALESCE(ti.invoice_id, ci.invoice_id) AS invoice_id,
        COALESCE(ti.product_name, ci.product_name) AS product_name,
        ti.quantity AS ti_quantity,
        ci.quantity AS ci_quantity,
        ti.unit_price AS ti_unit_price,
        ci.unit_price AS ci_unit_price,
        ti.total_price AS ti_total_price,
        ci.total_price AS ci_total_price,
        -- Flags to check existence easily
        (ti.invoice_id IS NOT NULL) AS exists_in_transact,
        (ci.invoice_id IS NOT NULL) AS exists_in_co_invoices
    FROM
        doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS ti
    FULL OUTER JOIN
        -- IMPORTANT: Using CO_INVOICES_ITEMS as per your last code block
        doc_ai_qs_db.doc_ai_schema.CO_INVOICES_ITEMS ci
    ON
        ti.invoice_id = ci.invoice_id
        AND ti.product_name IS NOT DISTINCT FROM ci.product_name -- Assuming product_name identifies the line item
);

-- Insert discrepancies into the review table
INSERT INTO doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_ITEMS (
    invoice_id,
    product_name,
    discrepancy_type,
    column_name,
    transact_items_value,
    co_invoices_items_value
)
-- 1. Check for Quantity mismatches (only where item exists in both tables)
SELECT
    invoice_id,
    product_name,
    'Value Mismatch' AS discrepancy_type,
    'quantity' AS column_name,
    CAST(ti_quantity AS VARCHAR) AS transact_items_value,
    CAST(ci_quantity AS VARCHAR) AS co_invoices_items_value
FROM JoinedItems
WHERE exists_in_transact AND exists_in_co_invoices -- Must exist in both for value mismatch
  AND ti_quantity IS DISTINCT FROM ci_quantity

UNION ALL

-- 2. Check for Unit Price mismatches (only where item exists in both tables)
SELECT
    invoice_id,
    product_name,
    'Value Mismatch' AS discrepancy_type,
    'unit_price' AS column_name,
    CAST(ti_unit_price AS VARCHAR) AS transact_items_value,
    CAST(ci_unit_price AS VARCHAR) AS co_invoices_items_value
FROM JoinedItems
WHERE exists_in_transact AND exists_in_co_invoices
  AND ti_unit_price IS DISTINCT FROM ci_unit_price

UNION ALL

-- 3. Check for Total Price mismatches (only where item exists in both tables)
SELECT
    invoice_id,
    product_name,
    'Value Mismatch' AS discrepancy_type,
    'total_price' AS column_name,
    CAST(ti_total_price AS VARCHAR) AS transact_items_value,
    CAST(ci_total_price AS VARCHAR) AS co_invoices_items_value
FROM JoinedItems
WHERE exists_in_transact AND exists_in_co_invoices
  AND ti_total_price IS DISTINCT FROM ci_total_price

UNION ALL

-- 4. Check for items missing in CO_INVOICES_ITEMS (exist only in TRANSACT_ITEMS)
SELECT
    invoice_id,
    product_name,
    'Missing Item' AS discrepancy_type,
    'Entire Row' AS column_name,
    'Exists' AS transact_items_value, -- Or display key values like CAST(ti_quantity AS VARCHAR) etc.
    'Missing' AS co_invoices_items_value
FROM JoinedItems
WHERE exists_in_transact AND NOT exists_in_co_invoices

UNION ALL

-- 5. Check for items missing in TRANSACT_ITEMS (exist only in CO_INVOICES_ITEMS)
SELECT
    invoice_id,
    product_name,
    'Missing Item' AS discrepancy_type,
    'Entire Row' AS column_name,
    'Missing' AS transact_items_value,
    'Exists' AS co_invoices_items_value -- Or display key values like CAST(ci_quantity AS VARCHAR) etc.
FROM JoinedItems
WHERE NOT exists_in_transact AND exists_in_co_invoices;




-------------



-- Create a target table specific to totals reconciliation
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS (
    invoice_id VARCHAR,                -- Identifier for the invoice being compared
    discrepancy_type VARCHAR,        -- 'Value Mismatch' or 'Missing Item'
    column_name VARCHAR,             -- Name of the column with the difference, or 'Entire Row' for missing items
    transact_totals_value VARCHAR,   -- Value from TRANSACT_TOTALS (casted to VARCHAR for consistency)
    co_invoices_totals_value VARCHAR,-- Value from CO_INVOICES_TOTALS (casted to VARCHAR)
    review_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
); 

-- Define the base join and comparison data in a CTE
CREATE OR REPLACE TEMPORARY TABLE JoinedTotals AS (
    SELECT
        COALESCE(tt.invoice_id, cit.invoice_id) AS invoice_id,
        tt.invoice_date AS tt_invoice_date,
        cit.invoice_date AS cit_invoice_date,
        tt.subtotal AS tt_subtotal,
        cit.subtotal AS cit_subtotal,
        tt.tax AS tt_tax,
        cit.tax AS cit_tax,
        tt.total AS tt_total,
        cit.total AS cit_total,
        -- Flags to check existence easily
        (tt.invoice_id IS NOT NULL) AS exists_in_transact,
        (cit.invoice_id IS NOT NULL) AS exists_in_co_invoices
    FROM
        doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS tt
    FULL OUTER JOIN
        doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS cit
    ON
        tt.invoice_id = cit.invoice_id -- Join only on invoice_id for totals tables
);

-- Insert discrepancies into the totals review table
INSERT INTO doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS (
    invoice_id,
    discrepancy_type,
    column_name,
    transact_totals_value,
    co_invoices_totals_value
)
-- 1. Check for invoice_date mismatches (only where invoice exists in both tables)
SELECT
    invoice_id,
    'Value Mismatch' AS discrepancy_type,
    'invoice_date' AS column_name,
    CAST(tt_invoice_date AS VARCHAR) AS transact_totals_value,
    CAST(cit_invoice_date AS VARCHAR) AS co_invoices_totals_value
FROM JoinedTotals
WHERE exists_in_transact AND exists_in_co_invoices -- Must exist in both for value mismatch
  AND tt_invoice_date IS DISTINCT FROM cit_invoice_date

UNION ALL

-- 2. Check for subtotal mismatches (only where invoice exists in both tables)
SELECT
    invoice_id,
    'Value Mismatch' AS discrepancy_type,
    'subtotal' AS column_name,
    CAST(tt_subtotal AS VARCHAR) AS transact_totals_value,
    CAST(cit_subtotal AS VARCHAR) AS co_invoices_totals_value
FROM JoinedTotals
WHERE exists_in_transact AND exists_in_co_invoices
  AND tt_subtotal IS DISTINCT FROM cit_subtotal

UNION ALL

-- 3. Check for tax mismatches (only where invoice exists in both tables)
SELECT
    invoice_id,
    'Value Mismatch' AS discrepancy_type,
    'tax' AS column_name,
    CAST(tt_tax AS VARCHAR) AS transact_totals_value,
    CAST(cit_tax AS VARCHAR) AS co_invoices_totals_value
FROM JoinedTotals
WHERE exists_in_transact AND exists_in_co_invoices
  AND tt_tax IS DISTINCT FROM cit_tax

UNION ALL

-- 4. Check for total mismatches (only where invoice exists in both tables)
SELECT
    invoice_id,
    'Value Mismatch' AS discrepancy_type,
    'total' AS column_name,
    CAST(tt_total AS VARCHAR) AS transact_totals_value,
    CAST(cit_total AS VARCHAR) AS co_invoices_totals_value
FROM JoinedTotals
WHERE exists_in_transact AND exists_in_co_invoices
  AND tt_total IS DISTINCT FROM cit_total

UNION ALL

-- 5. Check for invoices missing in CO_INVOICES_TOTALS (exist only in TRANSACT_TOTALS)
SELECT
    invoice_id,
    'Missing Item' AS discrepancy_type,
    'Entire Row' AS column_name,
    'Exists' AS transact_totals_value,
    'Missing' AS co_invoices_totals_value
FROM JoinedTotals
WHERE exists_in_transact AND NOT exists_in_co_invoices

UNION ALL

-- 6. Check for invoices missing in TRANSACT_TOTALS (exist only in CO_INVOICES_TOTALS)
SELECT
    invoice_id,
    'Missing Item' AS discrepancy_type,
    'Entire Row' AS column_name,
    'Missing' AS transact_totals_value,
    'Exists' AS co_invoices_totals_value
FROM JoinedTotals
WHERE NOT exists_in_transact AND exists_in_co_invoices; 


-- Optional: Query the results to verify
SELECT * FROM doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_ITEMS ORDER BY invoice_id, product_name, column_name;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS ORDER BY invoice_id, column_name;