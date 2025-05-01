-- Set the context to the correct database and schema (optional if already set in your session)
USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_ITEM_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
BEGIN
  -- Use MERGE to insert new discrepancies or update existing ones if re-run
  MERGE INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS AS target
  USING (
    -- Common Table Expression (CTE) to join the two item tables
    -- This CTE now includes row numbering to handle duplicate product names within the same invoice
    WITH NumberedTransactItems AS (
        SELECT
            ti.*,
            ROW_NUMBER() OVER (PARTITION BY invoice_id, product_name ORDER BY quantity, unit_price, total_price) as rn -- ## IMPORTANT ##: Replace ORDER BY with a stable column(s) if available (e.g., line_item_id, sequence_number) that defines the unique order of identical products within an invoice. Using value columns is a fallback and might be unreliable.
        FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS ti
    ),
    NumberedDocaiItems AS (
        SELECT
            ci.*,
            ROW_NUMBER() OVER (PARTITION BY invoice_id, product_name ORDER BY quantity, unit_price, total_price) as rn -- ## IMPORTANT ##: Use the EXACT SAME ORDER BY clause as in NumberedTransactItems for consistent matching.
        FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS ci
    ),
    JoinedItems AS (
      SELECT
        COALESCE(nti.invoice_id, ndi.invoice_id) AS invoice_id,
        COALESCE(nti.product_name, ndi.product_name) AS product_name,
        nti.quantity AS ti_quantity,
        ndi.quantity AS ci_quantity,
        nti.unit_price AS ti_unit_price,
        ndi.unit_price AS ci_unit_price,
        nti.total_price AS ti_total_price,
        ndi.total_price AS ci_total_price,
        -- Include the row number generated, might be useful for debugging mismatches
        COALESCE(nti.rn, ndi.rn) AS line_instance_number,
        -- Flags to check existence easily
        (nti.invoice_id IS NOT NULL) AS exists_in_transact,
        (ndi.invoice_id IS NOT NULL) AS exists_in_docai
      FROM
        NumberedTransactItems nti
      FULL OUTER JOIN
        NumberedDocaiItems ndi
      ON
        nti.invoice_id = ndi.invoice_id
        AND nti.product_name IS NOT DISTINCT FROM ndi.product_name -- Still match on product name
        AND nti.rn = ndi.rn -- Crucially, also match on the generated row number for uniqueness
    ),
    -- CTE to identify all potential discrepancies based on the improved join
    Discrepancies AS (
      -- 1. Check for Quantity mismatches
      SELECT
        invoice_id,
        product_name,
        line_instance_number, -- Include the generated number for reference
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Quantity Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'quantity',
            'original_value', ti_quantity::VARCHAR,
            'invoice_value', ci_quantity::VARCHAR,
            'line_instance', line_instance_number, -- Add line instance to details
            'message', 'Quantity does not match for this item instance.'
        ) AS item_mismatch_details
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai
        AND ti_quantity IS DISTINCT FROM ci_quantity

      UNION ALL

      -- 2. Check for Unit Price mismatches
      SELECT
        invoice_id,
        product_name,
        line_instance_number,
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Unit Price Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'unit_price',
            'original_value', ti_unit_price::VARCHAR,
            'invoice_value', ci_unit_price::VARCHAR,
            'line_instance', line_instance_number,
            'message', 'Unit price does not match for this item instance.'
        ) AS item_mismatch_details
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai
        AND ti_unit_price IS DISTINCT FROM ci_unit_price

      UNION ALL

      -- 3. Check for Total Price mismatches
      SELECT
        invoice_id,
        product_name,
        line_instance_number,
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Total Price Mismatch' AS discrepancy_type,
         OBJECT_CONSTRUCT(
            'field', 'total_price',
            'original_value', ti_total_price::VARCHAR,
            'invoice_value', ci_total_price::VARCHAR,
            'line_instance', line_instance_number,
            'message', 'Total price does not match for this item instance.'
        ) AS item_mismatch_details
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai
        AND ti_total_price IS DISTINCT FROM ci_total_price

      UNION ALL

      -- 4. Check for items missing in DOCAI_INVOICE_ITEMS
      SELECT
        invoice_id,
        product_name,
        line_instance_number,
        'Missing in Invoice' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Missing Item in Invoice' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'entire_item',
            'original_value', OBJECT_CONSTRUCT(
                'quantity', ti_quantity::VARCHAR,
                'unit_price', ti_unit_price::VARCHAR,
                'total_price', ti_total_price::VARCHAR
             ),
            'invoice_value', NULL,
            'line_instance', line_instance_number,
            'message', 'This specific item instance present in original data but missing in extracted invoice.'
        ) AS item_mismatch_details
      FROM JoinedItems
      WHERE exists_in_transact AND NOT exists_in_docai

      UNION ALL

      -- 5. Check for items missing in TRANSACT_ITEMS
      SELECT
        invoice_id,
        product_name,
        line_instance_number,
        'Missing in Original' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Missing Item in Original' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'entire_item',
            'original_value', NULL,
            'invoice_value', OBJECT_CONSTRUCT(
                'quantity', ci_quantity::VARCHAR,
                'unit_price', ci_unit_price::VARCHAR,
                'total_price', ci_total_price::VARCHAR
             ),
            'line_instance', line_instance_number,
            'message', 'This specific item instance present in extracted invoice but missing in original data.'
        ) AS item_mismatch_details
      FROM JoinedItems
      WHERE NOT exists_in_transact AND exists_in_docai
    )
    -- Select from the identified discrepancies to feed the MERGE statement
    SELECT * FROM Discrepancies
  ) AS source
  -- Update the MERGE condition to include line_instance_number if you want to track discrepancies at that level of granularity
  -- If item_mismatch_details includes the line_instance, matching on invoice, product, and field might still be sufficient.
  -- Let's keep the original MERGE condition for now, assuming the details object differentiates the lines.
  ON target.invoice_id = source.invoice_id
     AND target.product_name = source.product_name
     -- Match on the specific field discrepancy. The line_instance_number is now part of the details object.
     AND target.item_mismatch_details:field::VARCHAR = source.item_mismatch_details:field::VARCHAR
     AND target.item_mismatch_details:line_instance::NUMBER = source.line_instance_number -- Added matching on line instance number


  -- Action when a discrepancy for this item/field/line instance already exists
  WHEN MATCHED THEN UPDATE SET
    target.reconciliation_status = source.reconciliation_status,
    target.item_mismatch_details = source.item_mismatch_details,
    target.review_status = 'Pending Review', -- Reset review status if re-processed
    target.last_reconciled_timestamp = CURRENT_TIMESTAMP(),
    target.reviewed_by = NULL,
    target.reviewed_timestamp = NULL,
    target.corrected_invoice_number = NULL, -- Reset correction if re-processed
    target.notes = NULL

  -- Action when a new discrepancy for this item/field/line instance is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    product_name,
    -- line_instance_number, -- Optional: Add column to target table if needed
    reconciliation_status,
    item_mismatch_details,
    review_status,
    last_reconciled_timestamp
  ) VALUES (
    source.invoice_id,
    source.product_name,
    -- source.line_instance_number, -- Optional
    source.reconciliation_status,
    source.item_mismatch_details,
    source.review_status,
    CURRENT_TIMESTAMP()
  );

  -- Optional: Clean up old records (consider adding line_instance_number to the check if added to target table)
  -- ... (existing cleanup logic - adapt if necessary) ...

  status_message := 'Item reconciliation executed successfully. Discrepancies merged into RECONCILIATION_RESULTS using row numbering.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during item reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;

-- Create or replace the stored procedure for invoice totals reconciliation
CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_TOTALS_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
BEGIN
  -- Use MERGE to insert new discrepancies or update existing ones if re-run
  MERGE INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS AS target
  USING (
    -- Common Table Expression (CTE) to join the two totals tables
    WITH JoinedTotals AS (
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
        (cit.invoice_id IS NOT NULL) AS exists_in_docai
      FROM
        doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS tt -- Original totals
      FULL OUTER JOIN
        doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS cit -- Extracted invoice totals
      ON
        tt.invoice_id = cit.invoice_id
    ),
    -- CTE to identify all potential discrepancies at the totals level
    Discrepancies AS (
      -- 1. Check for Invoice Date mismatches
      SELECT
        invoice_id,
        'Discrepancy - Header' AS reconciliation_status, -- Status indicates header level
        'Pending Review' AS review_status,
        'Invoice Date Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'invoice_date',
            'original_value', tt_invoice_date::VARCHAR,
            'invoice_value', cit_invoice_date::VARCHAR,
            'message', 'Invoice date does not match.'
        ) AS item_mismatch_details -- Reusing column, JSON indicates field
      FROM JoinedTotals
      WHERE exists_in_transact AND exists_in_docai
        AND tt_invoice_date IS DISTINCT FROM cit_invoice_date

      UNION ALL

      -- 2. Check for Subtotal mismatches
      SELECT
        invoice_id,
        'Discrepancy - Header' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Subtotal Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'subtotal',
            'original_value', tt_subtotal::VARCHAR,
            'invoice_value', cit_subtotal::VARCHAR,
            'message', 'Subtotal does not match.'
        ) AS item_mismatch_details
      FROM JoinedTotals
      WHERE exists_in_transact AND exists_in_docai
        AND tt_subtotal IS DISTINCT FROM cit_subtotal

      UNION ALL

      -- 3. Check for Tax mismatches
      SELECT
        invoice_id,
        'Discrepancy - Header' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Tax Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'tax',
            'original_value', tt_tax::VARCHAR,
            'invoice_value', cit_tax::VARCHAR,
            'message', 'Tax amount does not match.'
        ) AS item_mismatch_details
      FROM JoinedTotals
      WHERE exists_in_transact AND exists_in_docai
        AND tt_tax IS DISTINCT FROM cit_tax

      UNION ALL

      -- 4. Check for Total mismatches
      SELECT
        invoice_id,
        'Discrepancy - Header' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Total Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'total',
            'original_value', tt_total::VARCHAR,
            'invoice_value', cit_total::VARCHAR,
            'message', 'Total amount does not match.'
        ) AS item_mismatch_details
      FROM JoinedTotals
      WHERE exists_in_transact AND exists_in_docai
        AND tt_total IS DISTINCT FROM cit_total

      UNION ALL

      -- 5. Check for invoices missing in DOCAI_INVOICE_TOTALS
      SELECT
        invoice_id,
        'Missing Invoice Header' AS reconciliation_status, -- Specific status
        'Pending Review' AS review_status,
        'Missing Invoice Header in DocAI' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'entire_header',
            'original_value', OBJECT_CONSTRUCT(
                'invoice_date', tt_invoice_date::VARCHAR,
                'subtotal', tt_subtotal::VARCHAR,
                'tax', tt_tax::VARCHAR,
                'total', tt_total::VARCHAR
             ),
            'invoice_value', NULL,
            'message', 'Invoice header present in original data but missing in extracted invoice totals.'
        ) AS item_mismatch_details
      FROM JoinedTotals
      WHERE exists_in_transact AND NOT exists_in_docai

      UNION ALL

      -- 6. Check for invoices missing in TRANSACT_TOTALS
      SELECT
        invoice_id,
        'Missing Original Header' AS reconciliation_status, -- Specific status
        'Pending Review' AS review_status,
        'Missing Invoice Header in Original' AS discrepancy_type,
        OBJECT_CONSTRUCT(
            'field', 'entire_header',
            'original_value', NULL,
            'invoice_value', OBJECT_CONSTRUCT(
                'invoice_date', cit_invoice_date::VARCHAR,
                'subtotal', cit_subtotal::VARCHAR,
                'tax', cit_tax::VARCHAR,
                'total', cit_total::VARCHAR
             ),
            'message', 'Invoice header present in extracted invoice totals but missing in original data.'
        ) AS item_mismatch_details
      FROM JoinedTotals
      WHERE NOT exists_in_transact AND exists_in_docai
    )
    -- Select from the identified discrepancies to feed the MERGE statement
    SELECT * FROM Discrepancies
  ) AS source
  -- Match condition for MERGE: based on invoice_id, the specific field in the details,
  ON target.invoice_id = source.invoice_id
     AND target.item_mismatch_details:field::VARCHAR = source.item_mismatch_details:field::VARCHAR

  -- Action when a discrepancy for this invoice/field already exists
  WHEN MATCHED THEN UPDATE SET
    target.reconciliation_status = source.reconciliation_status,
    target.item_mismatch_details = source.item_mismatch_details,
    target.review_status = 'Pending Review', -- Reset review status
    target.last_reconciled_timestamp = CURRENT_TIMESTAMP(),
    target.reviewed_by = NULL,
    target.reviewed_timestamp = NULL,
    target.corrected_invoice_number = NULL, -- Reset correction
    target.notes = NULL

  -- Action when a new discrepancy for this invoice/field is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    reconciliation_status,
    item_mismatch_details,
    review_status,
    last_reconciled_timestamp
  ) VALUES (
    source.invoice_id,
    source.reconciliation_status,
    source.item_mismatch_details,
    source.review_status,
    CURRENT_TIMESTAMP()
  );

  status_message := 'Totals reconciliation executed successfully. Discrepancies merged into RECONCILIATION_RESULTS.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during totals reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;

-- Example of how to call the stored procedure
-- CALL doc_ai_qs_db.doc_ai_schema.SP_RUN_TOTALS_RECONCILIATION();

-- Create the target table to store discrepancies
-- Using CREATE OR REPLACE TABLE allows rerunning the script without manual cleanup
-- CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED (
--     invoice_id VARCHAR,
--     transact_product_name VARCHAR,
--     co_invoice_product_name VARCHAR,
--     transact_quantity NUMBER, -- Adjust datatype if needed (e.g., DECIMAL, INTEGER)
--     co_invoice_quantity NUMBER, -- Adjust datatype if needed
--     transact_unit_price DECIMAL(18, 2), -- Use appropriate precision and scale
--     co_invoice_unit_price DECIMAL(18, 2), -- Use appropriate precision and scale
--     transact_total_price DECIMAL(18, 2), -- Use appropriate precision and scale
--     co_invoice_total_price DECIMAL(18, 2), -- Use appropriate precision and scale
--     review_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- Timestamp for when the record was added
-- );

----------
-- Redefine the target table to capture specific column discrepancies
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS (
    invoice_id VARCHAR,
    product_name VARCHAR,
    reconciliation_status VARCHAR,
    item_mismatch_details VARIANT,
    review_status VARCHAR,
    last_reconciled_timestamp TIMESTAMP_NTZ,
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    corrected_invoice_number VARCHAR,
    notes VARCHAR
);

-- Redefine the target table to capture specific column discrepancies
CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS (
    invoice_id VARCHAR,
    reconciliation_status VARCHAR,
    item_mismatch_details VARIANT,
    review_status VARCHAR,
    last_reconciled_timestamp TIMESTAMP_NTZ,
    reviewed_by VARCHAR,
    reviewed_timestamp TIMESTAMP_NTZ,
    corrected_invoice_number VARCHAR,
    notes VARCHAR
);

CALL SP_RUN_ITEM_RECONCILIATION();
SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS ORDER BY INVOICE_ID, PRODUCT_NAME;

CALL SP_RUN_TOTALS_RECONCILIATION();
SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS ORDER BY INVOICE_ID;

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
        doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS ci
    ON
        ti.invoice_id = ci.invoice_id
        AND ti.product_name IS NOT DISTINCT FROM ci.product_name -- Assuming product_name identifies the line item
);

-- -- Insert discrepancies into the review table
-- INSERT INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS (
--     invoice_id,
--     product_name,
--     discrepancy_type,
--     column_name,
--     transact_items_value,
--     co_invoices_items_value
-- )
-- -- 1. Check for Quantity mismatches (only where item exists in both tables)
-- SELECT
--     invoice_id,
--     product_name,
--     'Value Mismatch' AS discrepancy_type,
--     'quantity' AS column_name,
--     CAST(ti_quantity AS VARCHAR) AS transact_items_value,
--     CAST(ci_quantity AS VARCHAR) AS co_invoices_items_value
-- FROM JoinedItems
-- WHERE exists_in_transact AND exists_in_co_invoices -- Must exist in both for value mismatch
--   AND ti_quantity IS DISTINCT FROM ci_quantity

-- UNION ALL

-- -- 2. Check for Unit Price mismatches (only where item exists in both tables)
-- SELECT
--     invoice_id,
--     product_name,
--     'Value Mismatch' AS discrepancy_type,
--     'unit_price' AS column_name,
--     CAST(ti_unit_price AS VARCHAR) AS transact_items_value,
--     CAST(ci_unit_price AS VARCHAR) AS co_invoices_items_value
-- FROM JoinedItems
-- WHERE exists_in_transact AND exists_in_co_invoices
--   AND ti_unit_price IS DISTINCT FROM ci_unit_price

-- UNION ALL

-- -- 3. Check for Total Price mismatches (only where item exists in both tables)
-- SELECT
--     invoice_id,
--     product_name,
--     'Value Mismatch' AS discrepancy_type,
--     'total_price' AS column_name,
--     CAST(ti_total_price AS VARCHAR) AS transact_items_value,
--     CAST(ci_total_price AS VARCHAR) AS co_invoices_items_value
-- FROM JoinedItems
-- WHERE exists_in_transact AND exists_in_co_invoices
--   AND ti_total_price IS DISTINCT FROM ci_total_price

-- UNION ALL

-- -- 4. Check for items missing in CO_INVOICES_ITEMS (exist only in TRANSACT_ITEMS)
-- SELECT
--     invoice_id,
--     product_name,
--     'Missing Item' AS discrepancy_type,
--     'Entire Row' AS column_name,
--     'Exists' AS transact_items_value, -- Or display key values like CAST(ti_quantity AS VARCHAR) etc.
--     'Missing' AS co_invoices_items_value
-- FROM JoinedItems
-- WHERE exists_in_transact AND NOT exists_in_co_invoices

-- UNION ALL

-- -- 5. Check for items missing in TRANSACT_ITEMS (exist only in CO_INVOICES_ITEMS)
-- SELECT
--     invoice_id,
--     product_name,
--     'Missing Item' AS discrepancy_type,
--     'Entire Row' AS column_name,
--     'Missing' AS transact_items_value,
--     'Exists' AS co_invoices_items_value -- Or display key values like CAST(ci_quantity AS VARCHAR) etc.
-- FROM JoinedItems
-- WHERE NOT exists_in_transact AND exists_in_co_invoices
-- ORDER BY invoice_id, product_name, column_name;




-- -------------



-- -- Create a target table specific to totals reconciliation
-- CREATE OR REPLACE TABLE doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS (
--     invoice_id VARCHAR,                -- Identifier for the invoice being compared
--     discrepancy_type VARCHAR,        -- 'Value Mismatch' or 'Missing Item'
--     column_name VARCHAR,             -- Name of the column with the difference, or 'Entire Row' for missing items
--     transact_totals_value VARCHAR,   -- Value from TRANSACT_TOTALS (casted to VARCHAR for consistency)
--     co_invoices_totals_value VARCHAR,-- Value from CO_INVOICES_TOTALS (casted to VARCHAR)
--     review_generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
-- ); 

-- -- Define the base join and comparison data in a CTE
-- CREATE OR REPLACE TEMPORARY TABLE JoinedTotals AS (
--     SELECT
--         COALESCE(tt.invoice_id, cit.invoice_id) AS invoice_id,
--         tt.invoice_date AS tt_invoice_date,
--         cit.invoice_date AS cit_invoice_date,
--         tt.subtotal AS tt_subtotal,
--         cit.subtotal AS cit_subtotal,
--         tt.tax AS tt_tax,
--         cit.tax AS cit_tax,
--         tt.total AS tt_total,
--         cit.total AS cit_total,
--         -- Flags to check existence easily
--         (tt.invoice_id IS NOT NULL) AS exists_in_transact,
--         (cit.invoice_id IS NOT NULL) AS exists_in_co_invoices
--     FROM
--         doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS tt
--     FULL OUTER JOIN
--         doc_ai_qs_db.doc_ai_schema.CO_INVOICES_TOTALS cit
--     ON
--         tt.invoice_id = cit.invoice_id -- Join only on invoice_id for totals tables
-- );

-- -- Insert discrepancies into the totals review table
-- INSERT INTO doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS (
--     invoice_id,
--     discrepancy_type,
--     column_name,
--     transact_totals_value,
--     co_invoices_totals_value
-- )
-- -- 1. Check for invoice_date mismatches (only where invoice exists in both tables)
-- SELECT
--     invoice_id,
--     'Value Mismatch' AS discrepancy_type,
--     'invoice_date' AS column_name,
--     CAST(tt_invoice_date AS VARCHAR) AS transact_totals_value,
--     CAST(cit_invoice_date AS VARCHAR) AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE exists_in_transact AND exists_in_co_invoices -- Must exist in both for value mismatch
--   AND tt_invoice_date IS DISTINCT FROM cit_invoice_date

-- UNION ALL

-- -- 2. Check for subtotal mismatches (only where invoice exists in both tables)
-- SELECT
--     invoice_id,
--     'Value Mismatch' AS discrepancy_type,
--     'subtotal' AS column_name,
--     CAST(tt_subtotal AS VARCHAR) AS transact_totals_value,
--     CAST(cit_subtotal AS VARCHAR) AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE exists_in_transact AND exists_in_co_invoices
--   AND tt_subtotal IS DISTINCT FROM cit_subtotal

-- UNION ALL

-- -- 3. Check for tax mismatches (only where invoice exists in both tables)
-- SELECT
--     invoice_id,
--     'Value Mismatch' AS discrepancy_type,
--     'tax' AS column_name,
--     CAST(tt_tax AS VARCHAR) AS transact_totals_value,
--     CAST(cit_tax AS VARCHAR) AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE exists_in_transact AND exists_in_co_invoices
--   AND tt_tax IS DISTINCT FROM cit_tax

-- UNION ALL

-- -- 4. Check for total mismatches (only where invoice exists in both tables)
-- SELECT
--     invoice_id,
--     'Value Mismatch' AS discrepancy_type,
--     'total' AS column_name,
--     CAST(tt_total AS VARCHAR) AS transact_totals_value,
--     CAST(cit_total AS VARCHAR) AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE exists_in_transact AND exists_in_co_invoices
--   AND tt_total IS DISTINCT FROM cit_total

-- UNION ALL

-- -- 5. Check for invoices missing in CO_INVOICES_TOTALS (exist only in TRANSACT_TOTALS)
-- SELECT
--     invoice_id,
--     'Missing Item' AS discrepancy_type,
--     'Entire Row' AS column_name,
--     'Exists' AS transact_totals_value,
--     'Missing' AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE exists_in_transact AND NOT exists_in_co_invoices

-- UNION ALL

-- -- 6. Check for invoices missing in TRANSACT_TOTALS (exist only in CO_INVOICES_TOTALS)
-- SELECT
--     invoice_id,
--     'Missing Item' AS discrepancy_type,
--     'Entire Row' AS column_name,
--     'Missing' AS transact_totals_value,
--     'Exists' AS co_invoices_totals_value
-- FROM JoinedTotals
-- WHERE NOT exists_in_transact AND exists_in_co_invoices
-- ORDER BY invoice_id, column_name;


-- -- Optional: Query the results to verify


-- SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS;
-- SELECT * FROM doc_ai_qs_db.doc_ai_schema.TO_BE_REVIEWED_TOTALS;