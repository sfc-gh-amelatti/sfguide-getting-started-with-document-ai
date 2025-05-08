-- Set the context to the correct database and schema (optional if already set in your session)
USE ROLE doc_ai_qs_role;
USE WAREHOUSE doc_ai_qs_wh;
USE DATABASE doc_ai_qs_db;
USE SCHEMA doc_ai_schema;

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
     AND target.invoice_date IS NOT DISTINCT FROM source.invoice_date -- Handle potential NULL product names

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
              ROW_NUMBER() OVER (PARTITION BY ci.invoice_id ORDER BY ci.invoice_date, ci.subtotal, ci.tax, ci.total) as rn -- Use the EXACT SAME ORDER BY as before
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
  -- << MODIFIED >> Match on invoice, product, AND line instance number for uniqueness in Gold table
  -- ASSUMPTION: GOLD_INVOICE_TOTALS has a line_instance_number column (or similar)
  ON gold_target.invoice_id = gold_source.invoice_id
     --AND gold_target.product_name IS NOT DISTINCT FROM gold_source.product_name
     --AND gold_target.line_instance_number = gold_source.line_instance_number -- Use line instance number

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



CALL SP_RUN_ITEM_RECONCILIATION();
CALL SP_RUN_TOTALS_RECONCILIATION();
SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_ITEMS ORDER BY INVOICE_ID, PRODUCT_NAME;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_ITEMS;

SELECT * FROM doc_ai_qs_db.doc_ai_schema.RECONCILE_RESULTS_TOTALS ORDER BY INVOICE_ID;
SELECT * FROM doc_ai_qs_db.doc_ai_schema.GOLD_INVOICE_TOTALS;
