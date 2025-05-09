-- Set the context to the correct database and schema (optional if already set in your session)
USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;


CALL SP_RUN_ITEM_RECONCILIATION();
CALL SP_RUN_TOTALS_RECONCILIATION();
SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS ORDER BY INVOICE_ID, PRODUCT_NAME;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS ORDER BY INVOICE_ID, PRODUCT_NAME;

SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS ORDER BY INVOICE_ID;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS ORDER BY INVOICE_ID;



CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_ITEM_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
  current_run_timestamp TIMESTAMP_NTZ; -- Use consistent timestamp for the run
BEGIN
    current_run_timestamp := CURRENT_TIMESTAMP();
  -- Merge discrepancies and auto-reconciled items into the Silver reconciliation table
  MERGE INTO doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS AS target
  USING (
    -- Common Table Expression (CTE) to join the two item tables using row numbering
    WITH NumberedTransactItems AS (
        SELECT
            ti.*,
            ROW_NUMBER() OVER (PARTITION BY ti.invoice_id, ti.product_name ORDER BY ti.quantity, ti.unit_price, ti.total_price) as rn -- ## IMPORTANT ##: Stable ORDER BY preferred
        FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS ti
    ),
    NumberedDocaiItems AS (
        SELECT
            ci.*,
            ROW_NUMBER() OVER (PARTITION BY ci.invoice_id, ci.product_name ORDER BY ci.quantity, ci.unit_price, ci.total_price) as rn -- ## IMPORTANT ##: Use the EXACT SAME ORDER BY
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
        -- Include the row number generated for unique identification
        COALESCE(nti.rn, ndi.rn) AS line_instance_number,
        -- Use original source data for auto-reconciled items (using ndi as example)
        ndi.quantity AS reconciled_quantity,
        ndi.unit_price AS reconciled_unit_price,
        ndi.total_price AS reconciled_total_price,
        -- Flags to check existence easily
        (nti.invoice_id IS NOT NULL) AS exists_in_transact,
        (ndi.invoice_id IS NOT NULL) AS exists_in_docai
      FROM
        NumberedTransactItems nti
      FULL OUTER JOIN
        NumberedDocaiItems ndi
      ON
        nti.invoice_id = ndi.invoice_id
        AND nti.product_name IS NOT DISTINCT FROM ndi.product_name -- Match on product name
        AND nti.rn = ndi.rn -- Crucially, also match on the generated row number
    ),
    -- CTE to identify all potential discrepancies
    Discrepancies AS (
      -- 1. Quantity mismatches
      SELECT
        invoice_id, product_name, line_instance_number,
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Quantity Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT('field', 'quantity', 'original_value', ti_quantity::VARCHAR, 'invoice_value', ci_quantity::VARCHAR, 'line_instance', line_instance_number, 'message', 'Quantity does not match.') AS item_mismatch_details,
        NULL as quantity, NULL as unit_price, NULL as total_price -- No reconciled values for discrepancies
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai AND ti_quantity IS DISTINCT FROM ci_quantity
      UNION ALL
      -- 2. Unit Price mismatches
      SELECT
        invoice_id, product_name, line_instance_number,
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Unit Price Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT('field', 'unit_price', 'original_value', ti_unit_price::VARCHAR, 'invoice_value', ci_unit_price::VARCHAR, 'line_instance', line_instance_number, 'message', 'Unit price does not match.') AS item_mismatch_details,
        NULL, NULL, NULL
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai AND ti_unit_price IS DISTINCT FROM ci_unit_price
      UNION ALL
      -- 3. Total Price mismatches
      SELECT
        invoice_id, product_name, line_instance_number,
        'Discrepancy - Items' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Total Price Mismatch' AS discrepancy_type,
        OBJECT_CONSTRUCT('field', 'total_price', 'original_value', ti_total_price::VARCHAR, 'invoice_value', ci_total_price::VARCHAR, 'line_instance', line_instance_number, 'message', 'Total price does not match.') AS item_mismatch_details,
        NULL, NULL, NULL
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai AND ti_total_price IS DISTINCT FROM ci_total_price
      UNION ALL
      -- 4. Missing in DOCAI_INVOICE_ITEMS
      SELECT
        invoice_id, product_name, line_instance_number,
        'Missing in Invoice' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Missing Item in Invoice' AS discrepancy_type,
        OBJECT_CONSTRUCT('field', 'entire_item', 'original_value', OBJECT_CONSTRUCT('quantity', ti_quantity::VARCHAR, 'unit_price', ti_unit_price::VARCHAR, 'total_price', ti_total_price::VARCHAR), 'invoice_value', NULL, 'line_instance', line_instance_number, 'message', 'Item missing in extracted invoice.') AS item_mismatch_details,
        NULL, NULL, NULL
      FROM JoinedItems
      WHERE exists_in_transact AND NOT exists_in_docai
      UNION ALL
      -- 5. Missing in TRANSACT_ITEMS
      SELECT
        invoice_id, product_name, line_instance_number,
        'Missing in Original' AS reconciliation_status,
        'Pending Review' AS review_status,
        'Missing Item in Original' AS discrepancy_type,
        OBJECT_CONSTRUCT('field', 'entire_item', 'original_value', NULL, 'invoice_value', OBJECT_CONSTRUCT('quantity', ci_quantity::VARCHAR, 'unit_price', ci_unit_price::VARCHAR, 'total_price', ci_total_price::VARCHAR), 'line_instance', line_instance_number, 'message', 'Item missing in original data.') AS item_mismatch_details,
        NULL, NULL, NULL
      FROM JoinedItems
      WHERE NOT exists_in_transact AND exists_in_docai
    ),
    -- << NEW >> CTE to identify items that match perfectly
    MatchedItems AS (
      SELECT
        invoice_id,
        product_name,
        line_instance_number,
        'auto-reconciled' AS reconciliation_status, -- Set status for matched items
        'Auto-Reconciled' AS review_status,         -- Set review status
        NULL AS discrepancy_type,                 -- No discrepancy type
        NULL AS item_mismatch_details,            -- No mismatch details
        reconciled_quantity as quantity,          -- Keep the actual matched value
        reconciled_unit_price as unit_price,      -- Keep the actual matched value
        reconciled_total_price as total_price     -- Keep the actual matched value
      FROM JoinedItems
      WHERE exists_in_transact AND exists_in_docai
        AND ti_quantity IS NOT DISTINCT FROM ci_quantity
        AND ti_unit_price IS NOT DISTINCT FROM ci_unit_price
        AND ti_total_price IS NOT DISTINCT FROM ci_total_price
    ),
    -- << NEW >> Combine Discrepancies and Matched Items for the MERGE source
    ReconciliationSource AS (
      SELECT * FROM Discrepancies
      UNION ALL
      SELECT * FROM MatchedItems
    )
    -- Select final source for merge
    SELECT * FROM ReconciliationSource
  ) AS source
  -- << MODIFIED >> Use invoice_id, product_name, and line_instance_number as the unique key for merge
  ON target.invoice_id = source.invoice_id
     AND target.product_name IS NOT DISTINCT FROM source.product_name -- Handle potential NULL product names
     AND target.line_instance_number = source.line_instance_number

  -- << MODIFIED >> Action when a record for this item instance already exists
  WHEN MATCHED THEN UPDATE SET
    target.reconciliation_status = source.reconciliation_status,
    target.item_mismatch_details = source.item_mismatch_details, -- Update details if discrepancy changes or becomes null if matched
    -- Set review status based on whether it's a discrepancy or auto-reconciled
    target.review_status = CASE
                              WHEN source.reconciliation_status = 'auto-reconciled' THEN 'Auto-Reconciled'
                              WHEN target.review_status = 'Reviewed' THEN target.review_status
                              ELSE 'Pending Review' -- Reset to Pending Review if it becomes/remains a discrepancy
                           END,
    target.last_reconciled_timestamp = :current_run_timestamp, -- Use variable for consistency
    -- Reset review/correction fields only if it's now a discrepancy, keep them if auto-reconciled (though likely null anyway)
    target.reviewed_by = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_by 
                            WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL 
                            ELSE target.reviewed_by END,
    target.reviewed_timestamp = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_timestamp 
                                WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL
                                ELSE target.reviewed_timestamp END,
    target.corrected_invoice_number = CASE WHEN target.review_status = 'Reviewed' THEN target.corrected_invoice_number
                                    WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL                                
                                    ELSE target.corrected_invoice_number END,
    target.notes =  CASE WHEN target.review_status = 'Reviewed' THEN target.notes
                    WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL 
                    ELSE target.notes END,
    -- << NEW >> Update reconciled values if present (will be null for discrepancies)
    target.quantity = source.quantity,
    target.unit_price = source.unit_price,
    target.total_price = source.total_price


  -- << MODIFIED >> Action when a new discrepancy or auto-reconciled item is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    product_name,
    line_instance_number, -- Store the line instance number
    reconciliation_status,
    item_mismatch_details, -- Will be NULL for auto-reconciled
    review_status,         -- 'Pending Review' or 'Auto-Reconciled'
    last_reconciled_timestamp,
    quantity,              -- << NEW >> Store reconciled values if available
    unit_price,            -- << NEW >> Store reconciled values if available
    total_price            -- << NEW >> Store reconciled values if available
    -- reviewed_by, reviewed_timestamp, notes, etc remain NULL initially
  ) VALUES (
    source.invoice_id,
    source.product_name,
    source.line_instance_number,
    source.reconciliation_status,
    source.item_mismatch_details,
    source.review_status,
    :current_run_timestamp,  -- Use variable for consistency
    source.quantity,       -- Will be NULL for discrepancies
    source.unit_price,     -- Will be NULL for discrepancies
    source.total_price     -- Will be NULL for discrepancies
  );

  -- << NEW SECTION >> Merge fully auto-reconciled invoices into the Gold table
  -- Step 1: Identify invoices where ALL items are auto-reconciled in the results table
  -- Step 2: Select the item data for these invoices (using NumberedDocaiItems CTE)
  -- Step 3: Merge into the Gold table

  MERGE INTO doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS AS gold_target
  USING (
      WITH FullyReconciledInvoices AS (
          SELECT invoice_id
          FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS ri
          WHERE last_reconciled_timestamp = :current_run_timestamp -- Optional: Only consider invoices processed in this run
          GROUP BY invoice_id
          -- Having clause ensures only invoices where every single line item is 'auto-reconciled' are selected
          HAVING COUNT_IF(reconciliation_status != 'auto-reconciled') = 0
      ),
      -- Re-use NumberedDocaiItems or select directly if performance allows
      NumberedDocaiItems_ForGold AS (
          SELECT
              ci.*,
              ROW_NUMBER() OVER (PARTITION BY ci.invoice_id, ci.product_name ORDER BY ci.quantity, ci.unit_price, ci.total_price) as rn -- Use the EXACT SAME ORDER BY as before
          FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_ITEMS ci
          -- Only select items for invoices identified as fully reconciled
          INNER JOIN FullyReconciledInvoices fri ON ci.invoice_id = fri.invoice_id
      ),
      -- Prepare the final data structure for the Gold merge
      ReadyForGold AS (
          SELECT
              ndi.invoice_id,
              ndi.product_name,
              ndi.quantity,
              ndi.unit_price,
              ndi.total_price,
              'Auto-Reconciled' AS reviewed_by, -- Fixed value as per requirement
              :current_run_timestamp AS reviewed_timestamp, -- Use run timestamp
              'Auto-Reconciled' AS notes,       -- Fixed value as per requirement
              ndi.rn AS line_instance_number     -- Include the unique line identifier
          FROM NumberedDocaiItems_ForGold ndi
      )
      SELECT * FROM ReadyForGold
  ) AS gold_source
  -- << MODIFIED >> Match on invoice, product, AND line instance number for uniqueness in Gold table
  -- ASSUMPTION: GOLD_INVOICE_ITEMS has a line_instance_number column (or similar)
  ON gold_target.invoice_id = gold_source.invoice_id
     AND gold_target.product_name IS NOT DISTINCT FROM gold_source.product_name
     AND gold_target.line_instance_number = gold_source.line_instance_number -- Use line instance number

  -- Action if the auto-reconciled item already exists in Gold (e.g., re-processed)
  WHEN MATCHED THEN UPDATE SET
      gold_target.quantity = gold_source.quantity,           -- Update values in case they changed (though unlikely for auto-reconciled)
      gold_target.unit_price = gold_source.unit_price,
      gold_target.total_price = gold_source.total_price,
      gold_target.reviewed_by = gold_source.reviewed_by,     -- Ensure review status is set
      gold_target.reviewed_timestamp = gold_source.reviewed_timestamp, -- Update timestamp
      gold_target.notes = gold_source.notes                  -- Ensure notes are set

  -- Action if the auto-reconciled item is new to the Gold table
  WHEN NOT MATCHED THEN INSERT (
      invoice_id,
      product_name,
      quantity,
      unit_price,
      total_price,
      reviewed_by,
      reviewed_timestamp,
      notes,
      line_instance_number -- << NEW >> Insert the line instance number if the column exists
  ) VALUES (
      gold_source.invoice_id,
      gold_source.product_name,
      gold_source.quantity,
      gold_source.unit_price,
      gold_source.total_price,
      gold_source.reviewed_by,
      gold_source.reviewed_timestamp,
      gold_source.notes,
      gold_source.line_instance_number -- << NEW >> Value for the line instance number
  );

  -- Optional: Clean up old records (consider adding line_instance_number to the check if added to target table)
  -- ... (existing cleanup logic - adapt if necessary) ...

  status_message := 'Item reconciliation executed. Discrepancies and auto-reconciled items merged into RECONCILE_RESULTS_ITEMS. Fully auto-reconciled invoices merged into GOLD_INVOICE_ITEMS.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during item reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;


CREATE OR REPLACE PROCEDURE doc_ai_qs_db.doc_ai_schema.SP_RUN_TOTALS_RECONCILIATION()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  status_message VARCHAR;
  current_run_timestamp TIMESTAMP_NTZ; -- Use consistent timestamp for the run
BEGIN
    current_run_timestamp := CURRENT_TIMESTAMP();
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
        -- Use original source data for auto-reconciled items (using ndi as example)
        cit.invoice_date AS reconciled_date,
        cit.subtotal AS reconciled_subtotal,
        cit.tax AS reconciled_tax,
        cit.total AS reconciled_total,
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
            'original_value', tt_invoice_date::DATE,
            'invoice_value', cit_invoice_date::DATE,
            'message', 'Invoice date does not match.'
        ) AS item_mismatch_details, -- Reusing column, JSON indicates field,
        NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total -- No reconciled values for discrepancies
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
        ) AS item_mismatch_details,
        NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total
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
        ) AS item_mismatch_details,
        NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total
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
        ) AS item_mismatch_details,
        NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total
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
        ) AS item_mismatch_details,
        NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total
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
        ) AS item_mismatch_details,
            NULL as invoice_date, NULL as subtotal, NULL as tax, NULL as total
      FROM JoinedTotals
      WHERE NOT exists_in_transact AND exists_in_docai
    ),

    MatchedItems AS (
      SELECT
        invoice_id,
        'auto-reconciled' AS reconciliation_status, -- Set status for matched items
        'Auto-Reconciled' AS review_status,         -- Set review status
        NULL AS discrepancy_type,                 -- No discrepancy type
        CAST(NULL AS OBJECT) AS item_mismatch_details,            -- No mismatch details
        reconciled_date AS invoice_date,
        reconciled_subtotal AS subtotal,
        reconciled_tax AS tax,
        reconciled_total as total,
      FROM JoinedTotals
      WHERE exists_in_transact AND exists_in_docai
        AND tt_invoice_date IS NOT DISTINCT FROM cit_invoice_date
        AND tt_subtotal IS NOT DISTINCT FROM cit_subtotal
        AND tt_tax IS NOT DISTINCT FROM cit_tax
        AND tt_total IS NOT DISTINCT FROM cit_total
    ),
    ReconciliationSource AS (
        SELECT * FROM Discrepancies
        UNION ALL
        SELECT * FROM MatchedItems
    )
-- Select final source for merge
    SELECT DISTINCT * FROM ReconciliationSource
  ) AS source
  -- << MODIFIED >> Use invoice_id, product_name, and line_instance_number as the unique key for merge
  ON target.invoice_id = source.invoice_id
     --AND target.invoice_date IS NOT DISTINCT FROM source.invoice_date -- Handle potential NULL product names

  -- << MODIFIED >> Action when a record for this item instance already exists
  WHEN MATCHED THEN UPDATE SET
    target.reconciliation_status = source.reconciliation_status,
    target.item_mismatch_details = source.item_mismatch_details, -- Update details if discrepancy changes or becomes null if matched
    -- Set review status based on whether it's a discrepancy or auto-reconciled
    target.review_status = CASE
                              WHEN source.reconciliation_status = 'auto-reconciled' THEN 'Auto-Reconciled'
                              WHEN target.review_status = 'Reviewed' THEN target.review_status
                              ELSE 'Pending Review' -- Reset to Pending Review if it becomes/remains a discrepancy
                           END,
    target.last_reconciled_timestamp = :current_run_timestamp, -- Use variable for consistency
    -- Reset review/correction fields only if it's now a discrepancy, keep them if auto-reconciled (though likely null anyway)
    target.reviewed_by = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_by
                        WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL 
                        ELSE target.reviewed_by END,
    target.reviewed_timestamp = CASE WHEN target.review_status = 'Reviewed' THEN target.reviewed_timestamp
                        WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL 
                        ELSE target.reviewed_timestamp END,
    target.corrected_invoice_number = CASE WHEN target.review_status = 'Reviewed' THEN target.corrected_invoice_number
                        WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL ELSE target.corrected_invoice_number END,
    target.notes = CASE WHEN target.review_status = 'Reviewed' THEN target.notes
                        WHEN source.reconciliation_status != 'auto-reconciled' THEN NULL ELSE target.notes END,
    -- << NEW >> Update reconciled values if present (will be null for discrepancies)
    target.invoice_date = source.invoice_date,
    target.subtotal = source.subtotal,
    target.tax = source.tax,
    target.total = source.total


  -- << MODIFIED >> Action when a new discrepancy or auto-reconciled item is found
  WHEN NOT MATCHED THEN INSERT (
    invoice_id,
    invoice_date,
    reconciliation_status,
    item_mismatch_details, -- Will be NULL for auto-reconciled
    review_status,         -- 'Pending Review' or 'Auto-Reconciled'
    last_reconciled_timestamp,
    subtotal,              -- << NEW >> Store reconciled values if available
    tax,            -- << NEW >> Store reconciled values if available
    total            -- << NEW >> Store reconciled values if available
    -- reviewed_by, reviewed_timestamp, notes, etc remain NULL initially
  ) VALUES (
    source.invoice_id,
    source.invoice_date,
    source.reconciliation_status,
    source.item_mismatch_details,
    source.review_status,
    :current_run_timestamp,  -- Use variable for consistency
    source.subtotal,       -- Will be NULL for discrepancies
    source.tax,     -- Will be NULL for discrepancies
    source.total     -- Will be NULL for discrepancies
  );

  -- << NEW SECTION >> Merge fully auto-reconciled invoices into the Gold table
  -- Step 1: Identify invoices where ALL items are auto-reconciled in the results table
  -- Step 2: Select the item data for these invoices (using NumberedDocaiItems CTE)
  -- Step 3: Merge into the Gold table

 MERGE INTO doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS AS gold_target
  USING (
      WITH FullyReconciledInvoices AS (
          SELECT invoice_id
          FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS ri
          WHERE last_reconciled_timestamp = :current_run_timestamp -- Optional: Only consider invoices processed in this run
          GROUP BY invoice_id
          -- Having clause ensures only invoices where every single line item is 'auto-reconciled' are selected
          HAVING COUNT_IF(reconciliation_status != 'auto-reconciled') = 0
      ),
      -- Re-use NumberedDocaiItems or select directly if performance allows
      NumberedDocaiItems_ForGold AS (
          SELECT
              ci.*,
              --ROW_NUMBER() OVER (PARTITION BY ci.invoice_id ORDER BY ci.invoice_date, ci.subtotal, ci.tax, ci.total) as rn -- Use the EXACT SAME ORDER BY as before
          FROM doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS ci
          -- Only select items for invoices identified as fully reconciled
          INNER JOIN FullyReconciledInvoices fri ON ci.invoice_id = fri.invoice_id
      ),
      -- Prepare the final data structure for the Gold merge
      ReadyForGold AS (
          SELECT
              ndi.invoice_id,
              ndi.invoice_date,
              ndi.subtotal,
              ndi.tax,
              ndi.total,
              'Auto-Reconciled' AS reviewed_by, -- Fixed value as per requirement
              :current_run_timestamp AS reviewed_timestamp, -- Use run timestamp
              'Auto-Reconciled' AS notes,       -- Fixed value as per requirement
              --ndi.rn AS line_instance_number     -- Include the unique line identifier
          FROM NumberedDocaiItems_ForGold ndi
      )
      SELECT * FROM ReadyForGold
  ) AS gold_source
  -- << MODIFIED >>
  -- ASSUMPTION: GOLD_INVOICE_TOTALS
  ON gold_target.invoice_id = gold_source.invoice_id

  -- Action if the auto-reconciled item already exists in Gold (e.g., re-processed)
  WHEN MATCHED THEN UPDATE SET
      gold_target.invoice_date = gold_source.invoice_date,           -- Update values in case they changed (though unlikely for auto-reconciled)
      gold_target.subtotal = gold_source.subtotal,
      gold_target.tax = gold_source.tax,
      gold_target.total = gold_source.total,
      gold_target.reviewed_by = gold_source.reviewed_by,     -- Ensure review status is set
      gold_target.reviewed_timestamp = gold_source.reviewed_timestamp, -- Update timestamp
      gold_target.notes = gold_source.notes                  -- Ensure notes are set

  -- Action if the auto-reconciled item is new to the Gold table
  WHEN NOT MATCHED THEN INSERT (
      invoice_id,
      invoice_date,
      subtotal,
      tax,
      total,
      reviewed_by,
      reviewed_timestamp,
      notes
      --line_instance_number -- << NEW >> Insert the line instance number if the column exists
  ) VALUES (
      gold_source.invoice_id,
      gold_source.invoice_date,
      gold_source.subtotal,
      gold_source.tax,
      gold_source.total,
      gold_source.reviewed_by,
      gold_source.reviewed_timestamp,
      gold_source.notes
      --gold_source.line_instance_number -- << NEW >> Value for the line instance number
  );

  -- Optional: Clean up old records (consider adding line_instance_number to the check if added to target table)
  -- ... (existing cleanup logic - adapt if necessary) ...

  status_message := 'Totals reconciliation executed. Discrepancies and auto-reconciled totals merged into RECONCILE_RESULTS_TOTALS. Fully auto-reconciled invoices merged into GOLD_INVOICE_TOTALS.';
  RETURN status_message;

EXCEPTION
  WHEN OTHER THEN
    status_message := 'Error during totals reconciliation: ' || SQLERRM;
    RETURN status_message; -- Or handle error logging appropriately
END;
$$;

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
    notes VARCHAR,
    line_instance_number NUMBER,
    quantity NUMBER,            
    unit_price DECIMAL(12,2),
    total_price DECIMAL(12,2)
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
    notes VARCHAR,
    invoice_date DATE,
    subtotal DECIMAL(10,2),
    tax DECIMAL(10,2),
    total DECIMAL(10,2)
    
);

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
    line_instance_number NUMBER
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
    notes VARCHAR -- Link to notes in silver or separate notes field
    -- line_instance_number NUMBER
);

-- CREATE A STREAM TO MONITOR THE Bronze db table for new items to pass to our reconciliation task
CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DB_STREAM 
ON TABLE doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS;

CREATE OR REPLACE STREAM doc_ai_qs_db.doc_ai_schema.BRONZE_DOCAI_STREAM 
ON TABLE doc_ai_qs_db.doc_ai_schema.DOCAI_INVOICE_TOTALS;

-- CREATE A TASK TO RUN WHEN THE STREAM DETECTS NEW INFO IN OUR MAIN DB TABLE OR DOCAI TABLE
create or replace task doc_ai_qs_db.doc_ai_schema.RECONCILE
	warehouse=doc_ai_qs_wh
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('BRONZE_DB_STREAM') OR SYSTEM$STREAM_HAS_DATA('BRONZE_DOCAI_STREAM')
	as BEGIN
        CALL SP_RUN_ITEM_RECONCILIATION();
        CALL SP_RUN_TOTALS_RECONCILIATION();
    END;

ALTER TASK doc_ai_qs_db.doc_ai_schema.RECONCILE RESUME;


--Kick off our streams + tasks with data entering the original bronze db tables.
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