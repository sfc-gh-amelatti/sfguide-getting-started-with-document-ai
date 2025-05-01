import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp, sql_expr
import snowflake.snowpark as snowpark # Required for types like DataFrame
import pandas as pd
from datetime import datetime
import pypdfium2 as pdfium # Import pypdfium2

st.set_page_config(layout="wide") # Use wider layout for tables

# --- Configuration ---
DB_NAME = "doc_ai_qs_db"
SCHEMA_NAME = "doc_ai_schema"
STAGE_NAME = "DOC_AI_STAGE"

SILVER_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_ITEMS"
SILVER_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.RECONCILE_RESULTS_TOTALS"

BRONZE_TRANSACT_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_ITEMS"
BRONZE_TRANSACT_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.TRANSACT_TOTALS"
BRONZE_DOCAI_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_ITEMS"
BRONZE_DOCAI_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.DOCAI_INVOICE_TOTALS"

GOLD_ITEMS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_ITEMS"
GOLD_TOTALS_TABLE = f"{DB_NAME}.{SCHEMA_NAME}.GOLD_INVOICE_TOTALS"

# --- Get Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Snowflake session established!")
    CURRENT_USER = 'instacart_admin'
    #CURRENT_USER = session.sql("SELECT CURRENT_USER()").replace("'", "''") # Get current user for audit and escape quotes
except Exception as e:
    st.error(f"Error getting Snowflake session: {e}")
    st.stop() # Stop execution if session cannot be established

# --- PDF Display Functions ---

def display_pdf_page():
    """Renders and displays the current PDF page."""
    if 'pdf_doc' not in st.session_state or st.session_state['pdf_doc'] is None:
        st.warning("No PDF document loaded.")
        return
    if 'pdf_page' not in st.session_state:
         st.session_state['pdf_page'] = 0 # Initialize if missing

    pdf = st.session_state['pdf_doc']
    page_index = st.session_state['pdf_page']
    num_pages = len(pdf)

    if not 0 <= page_index < num_pages:
        st.error(f"Invalid page index: {page_index}. Must be between 0 and {num_pages-1}.")
        st.session_state['pdf_page'] = 0 # Reset to first page
        page_index = 0

    page = pdf[page_index]

    try:
        # Rendering parameters - adjust scale for performance/quality trade-off
        bitmap = page.render(
                        scale = 2, # Reduced scale from 8 for potentially better performance
                        rotation = 0,
                )
        pil_image = bitmap.to_pil()
        st.image(pil_image, use_container_width='always') # Display the rendered page image
    except Exception as e:
        st.error(f"Error rendering PDF page {page_index + 1}: {e}")

def previous_pdf_page():
    """Navigates to the previous PDF page."""
    if 'pdf_page' in st.session_state and st.session_state['pdf_page'] > 0:
        st.session_state['pdf_page'] -= 1

def next_pdf_page():
    """Navigates to the next PDF page."""
    if ('pdf_page' in st.session_state and
        'pdf_doc' in st.session_state and
        st.session_state['pdf_doc'] is not None and
        st.session_state['pdf_page'] < len(st.session_state['pdf_doc']) - 1):
        st.session_state['pdf_page'] += 1

# --- Helper Functions ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_silver_data(status_filter='Pending Review'):
    """Loads data from silver reconciliation tables, optionally filtering by review_status."""
    try:
        # Use UNION ALL to get all invoice IDs needing review from both tables
        # Select distinct invoice IDs to avoid duplicates if an invoice has issues in both items and totals
        query = f"""
        SELECT DISTINCT invoice_id, reconciliation_status, review_status, last_reconciled_timestamp
        FROM {SILVER_ITEMS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        UNION
        SELECT DISTINCT invoice_id, reconciliation_status, review_status, last_reconciled_timestamp
        FROM {SILVER_TOTALS_TABLE}
        WHERE review_status = '{status_filter}' OR '{status_filter}' = 'All'
        ORDER BY last_reconciled_timestamp DESC
        """
        silver_df = session.sql(query).to_pandas()

        # Also load full details for display later if needed (optional here, load on demand below)
        silver_items_full_df = session.table(SILVER_ITEMS_TABLE).filter(col("review_status") == status_filter).to_pandas() if status_filter != 'All' else session.table(SILVER_ITEMS_TABLE).to_pandas()
        silver_totals_full_df = session.table(SILVER_TOTALS_TABLE).filter(col("review_status") == status_filter).to_pandas() if status_filter != 'All' else session.table(SILVER_TOTALS_TABLE).to_pandas()

        return silver_df, silver_items_full_df, silver_totals_full_df
    except Exception as e:
        st.error(f"Error loading Silver data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame() # Return empty DataFrames on error

def load_bronze_data(invoice_id):
    """Loads data from all relevant bronze tables for a specific invoice_id."""
    bronze_data = {}
    try:
        if invoice_id:
            # Use .filter().to_pandas() for smaller datasets typical for one invoice
            bronze_data['transact_items'] = session.table(BRONZE_TRANSACT_ITEMS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['transact_totals'] = session.table(BRONZE_TRANSACT_TOTALS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['docai_items'] = session.table(BRONZE_DOCAI_ITEMS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
            bronze_data['docai_totals'] = session.table(BRONZE_DOCAI_TOTALS_TABLE).filter(col("invoice_id") == invoice_id).to_pandas()
        return bronze_data
    except Exception as e:
        st.error(f"Error loading Bronze data for invoice {invoice_id}: {e}")
        return {} # Return empty dict on error

# --- Streamlit App UI ---
st.title("🛒 Instacart Invoice Reconciliation")
st.markdown(f"Connected as user: **{CURRENT_USER}**")

# --- Section 1: Display Silver Tables & Select Invoice ---
st.header("1. Invoices Awaiting Review")

review_status_options = ['Pending Review', 'Reviewed', 'Auto-Reconciled']
selected_status = st.selectbox("Filter by Review Status:", review_status_options, index=0) # Default to 'Pending Review'

# Load distinct invoice IDs based on filter
invoices_to_review_df, silver_items_details_df, silver_totals_details_df = load_silver_data(selected_status)

if not invoices_to_review_df.empty:
    st.write(f"Found {len(invoices_to_review_df['INVOICE_ID'].unique())} unique invoices with status '{selected_status}'.")

    # Allow user to select an invoice
    invoice_list = [""] + invoices_to_review_df['INVOICE_ID'].unique().tolist() # Add blank option
    selected_invoice_id = st.selectbox(
        "Select Invoice ID to Review/Correct:",
        invoice_list,
        index=0, # Default to blank
        key="invoice_selector"
    )

    # Display details from Silver tables for context (optional)
    with st.expander("Show Silver Reconciliation Details for All Filtered Invoices"):
         st.subheader("Silver - Item Reconciliation Details")
         st.dataframe(silver_items_details_df, use_container_width=True)
         st.subheader("Silver - Total Reconciliation Details")
         st.dataframe(silver_totals_details_df, use_container_width=True)

else:
    st.info(f"No invoices found with status '{selected_status}'.")
    selected_invoice_id = None # Ensure no invoice is selected if list is empty

# --- Section 2: Display Bronze Data for Selected Invoice ---
st.header("2. Review and Correct Invoice Data")

if selected_invoice_id:
    st.subheader(f"Displaying Data for Invoice: `{selected_invoice_id}`")

    # Load data from Bronze layer
    bronze_data_dict = load_bronze_data(selected_invoice_id)

    if bronze_data_dict:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Original Data (Source: TRANSACT_*)**")
            st.caption("Editable Fields - These will be saved to the Gold layer.")

            # --- Editable Transact Items ---
            st.write("**Items:**")
            if not bronze_data_dict['transact_items'].empty:
                 # Convert relevant columns to numeric for proper editing if they aren't already
                 for col_name in ['QUANTITY', 'UNIT_PRICE', 'TOTAL_PRICE']:
                    if col_name in bronze_data_dict['transact_items'].columns:
                        bronze_data_dict['transact_items'][col_name] = pd.to_numeric(bronze_data_dict['transact_items'][col_name], errors='coerce')

                 edited_transact_items_df = st.data_editor(
                    bronze_data_dict['transact_items'],
                    key="editor_transact_items",
                    num_rows="dynamic", # Allow adding/deleting rows
                    use_container_width=True,
                    column_config={ # Optional: Add specific configs for better editing
                        "UNIT_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                        "TOTAL_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                    }
                )
                 st.session_state.edited_transact_items = edited_transact_items_df # Store edited data
            else:
                 st.info("No data found in TRANSACT_ITEMS for this invoice.")
                 # Provide an empty editor if needed for adding completely new data
                 # st.session_state.edited_transact_items = st.data_editor(pd.DataFrame(columns=['invoice_id', 'product_name', 'quantity', 'unit_price', 'total_price']), num_rows="dynamic", key="editor_transact_items_new")


            # --- Editable Transact Totals ---
            st.write("**Totals:**")
            if not bronze_data_dict['transact_totals'].empty:
                # Ensure only one row and make it editable
                transact_totals_edit_df = bronze_data_dict['transact_totals'].head(1).copy() # Take first row if multiple exist

                # Convert relevant columns to numeric/date
                for col_name in ['SUBTOTAL', 'TAX', 'TOTAL']:
                     if col_name in transact_totals_edit_df.columns:
                        transact_totals_edit_df[col_name] = pd.to_numeric(transact_totals_edit_df[col_name], errors='coerce')
                if 'INVOICE_DATE' in transact_totals_edit_df.columns:
                    transact_totals_edit_df['INVOICE_DATE'] = pd.to_datetime(transact_totals_edit_df['INVOICE_DATE'], errors='coerce').dt.date


                edited_transact_totals_df = st.data_editor(
                    transact_totals_edit_df,
                    key="editor_transact_totals",
                    hide_index=True,
                    use_container_width=True,
                    disabled=["invoice_id"], # Don't allow editing invoice ID here
                     column_config={ # Optional: Add specific configs for better editing
                        "INVOICE_DATE": st.column_config.DateColumn(format="YYYY-MM-DD"),
                        "SUBTOTAL": st.column_config.NumberColumn(format="$%.2f"),
                        "TAX": st.column_config.NumberColumn(format="$%.2f"),
                        "TOTAL": st.column_config.NumberColumn(format="$%.2f"),
                    }
                )
                st.session_state.edited_transact_totals = edited_transact_totals_df # Store edited data
            else:
                st.info("No data found in TRANSACT_TOTALS for this invoice.")
                 # Provide an empty editor if needed
                 # st.session_state.edited_transact_totals = st.data_editor(pd.DataFrame(columns=['invoice_id', 'invoice_date', 'subtotal', 'tax', 'total']), hide_index=True, key="editor_transact_totals_new")


        with col2:
            st.markdown("**Reference Data (Source: DOCAI_*)**")
            st.caption("Read-only reference extracted by Document AI.")

            # --- Display DocAI Items ---
            st.write("**Items (DocAI):**")
            if not bronze_data_dict['docai_items'].empty:
                st.dataframe(bronze_data_dict['docai_items'], use_container_width=True)
            else:
                st.info("No data found in DOCAI_INVOICE_ITEMS for this invoice.")

            # --- Display DocAI Totals ---
            st.write("**Totals (DocAI):**")
            if not bronze_data_dict['docai_totals'].empty:
                st.dataframe(bronze_data_dict['docai_totals'], use_container_width=True)
                pdf_file_name = bronze_data_dict['docai_totals']['FILE_NAME'][0]
            else:
                st.info("No data found in DOCAI_INVOICE_TOTALS for this invoice.")
                pdf_file_name = ""

        # Initialize session state keys for PDF viewer if they don't exist
        if 'pdf_page' not in st.session_state:
            st.session_state['pdf_page'] = 0
        if 'pdf_doc' not in st.session_state:
            st.session_state['pdf_doc'] = None
        if 'pdf_url' not in st.session_state:
            st.session_state['pdf_url'] = None

        st.subheader("Associated Document")
        # --- New PDF Display Logic ---
        if selected_invoice_id:
            # pdf_file_name = f"{selected_row['INVOICE_ID']}.pdf" # Assumed naming convention
            #pdf_file_name = "Invoice_3.pdf" # Assumed naming convention
            # try:
            #     pdf_file_name = bronze_data_dict['docai_totals']['FILE_NAME'][0]
            # except: 
            #     pdf_file_name = ""
                
            stage_path = f"@{DB_NAME}.{SCHEMA_NAME}.{STAGE_NAME}/{pdf_file_name}"

            # Check if we need to load a new PDF
            if (st.session_state['pdf_url'] != stage_path):
                st.write(f"Loading PDF from: {stage_path}")
                try:
                    pdf_stream = session.file.get_stream(stage_path, decompress=False)
                    pdf_bytes = pdf_stream.read() # Read into memory first
                    pdf_stream.close()
                    st.session_state['pdf_doc'] = pdfium.PdfDocument(pdf_bytes) # Load from bytes
                    st.session_state['pdf_url'] = stage_path
                    st.session_state['pdf_page'] = 0 # Reset to first page
                    st.success(f"Loaded '{pdf_file_name}'")
                except Exception as e:
                    if pdf_file_name == "":
                        st.warning(f"The file you're looking for may not have been uploaded yet!")
                    else:
                        st.warning(f"Failed to load or read PDF from '{stage_path}': {e}")
                        st.session_state['pdf_doc'] = None # Ensure pdf_doc is None on failure
                        st.session_state['pdf_url'] = None

            # Display navigation and page if PDF is loaded
            if st.session_state.get('pdf_doc') is not None:
                nav_col1, nav_col2, nav_col3 = st.columns([1,2,1]) # Adjusted column ratios
                with nav_col1:
                    st.button("⏮️ Previous", on_click=previous_pdf_page, use_container_width=True)
                with nav_col2:
                    st.write(f"<div style='text-align: center;'>Page {st.session_state['pdf_page'] + 1} of {len(st.session_state['pdf_doc'])}</div>", unsafe_allow_html=True)
                with nav_col3:
                    st.button("Next ⏭️", on_click=next_pdf_page, use_container_width=True)

                display_pdf_page() # Call the function to render the page
            else:
                 st.info("Could not load document preview.")

        else:
            st.warning("No Invoice ID found for the selected row to retrieve the document.")
            # Clear PDF state if nothing is selected
            st.session_state['pdf_doc'] = None
            st.session_state['pdf_url'] = None
            st.session_state['pdf_page'] = 0


        
        # --- Section 3: Submit Corrections ---
        st.header("3. Submit Review and Corrections")

        # Add fields for notes and corrected invoice number (if applicable)
        review_notes = st.text_area("Review Notes / Comments:", key="review_notes")
        corrected_invoice_num = st.text_input("Corrected Invoice Number (if applicable):", key="corrected_inv_num") # Optional

        submit_button = st.button("✅ Submit Review and Save Corrections to Gold Layer")

        if submit_button:
            # --- Data Validation (Basic Example) ---
            valid = True
            if 'edited_transact_items' not in st.session_state or st.session_state.edited_transact_items.empty:
                st.warning("No item data to submit.")
                # valid = False # Decide if this is an error or acceptable
            if 'edited_transact_totals' not in st.session_state or st.session_state.edited_transact_totals.empty:
                 st.warning("No total data to submit.")
                 # valid = False # Decide if this is an error or acceptable

            if valid:
                try:
                    st.write("Submitting...")
                    current_ts = datetime.now() # Use current time for reviewed_timestamp

                    # --- Prepare Data for Gold Layer ---
                    # Items
                    if 'edited_transact_items' in st.session_state and not st.session_state.edited_transact_items.empty:
                        gold_items_df = st.session_state.edited_transact_items.copy()
                        # Ensure invoice_id is consistent if rows were added/modified
                        gold_items_df['INVOICE_ID'] = selected_invoice_id
                        gold_items_df['REVIEWED_BY'] = CURRENT_USER
                        gold_items_df['REVIEWED_TIMESTAMP'] = current_ts
                        # Add original DOCAI values if needed/available in your schema
                        # gold_items_df['original_docai_...'] = ...
                        gold_items_df['NOTES'] = review_notes # Add notes
                        gold_items_df['LINE_INSTANCE_NUMBER'] = 999

                         # Convert Pandas DataFrame to Snowpark DataFrame for writing
                        snowpark_gold_items = session.create_dataframe(gold_items_df)
                        # Convert column names to uppercase to match Snowflake default behavior if needed
                        for col_name in snowpark_gold_items.columns:
                             snowpark_gold_items = snowpark_gold_items.with_column_renamed(col_name, col_name.upper())

                        # Write to Gold Items Table (Append Mode)
                        # It's often better to delete existing corrected data for this invoice first
                        # Or use MERGE INTO for more robust upsert logic
                        st.write(f"Writing {len(snowpark_gold_items.collect())} rows to {GOLD_ITEMS_TABLE}...") # Use collect() carefully for large data
                        # Example: Delete existing before insert (use with caution)
                        session.sql(f"DELETE FROM {GOLD_ITEMS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'").collect()
                        snowpark_gold_items.write.mode("append").save_as_table(GOLD_ITEMS_TABLE)
                        st.success(f"Successfully saved corrected items to {GOLD_ITEMS_TABLE}")

                    # Totals
                    if 'edited_transact_totals' in st.session_state and not st.session_state.edited_transact_totals.empty:
                        gold_totals_df = st.session_state.edited_transact_totals.copy()
                         # Ensure invoice_id is consistent
                        gold_totals_df['INVOICE_ID'] = selected_invoice_id
                        gold_totals_df['REVIEWED_BY'] = CURRENT_USER
                        gold_totals_df['REVIEWED_TIMESTAMP'] = current_ts
                        # Add original DOCAI values if needed
                        gold_totals_df['NOTES'] = review_notes
                        #gold_totals_df['LINE_INSTANCE_NUMBER'] = ""

                        # Convert Pandas DataFrame to Snowpark DataFrame for writing
                        snowpark_gold_totals = session.create_dataframe(gold_totals_df)
                        # Convert column names to uppercase
                        for col_name in snowpark_gold_totals.columns:
                             snowpark_gold_totals = snowpark_gold_totals.with_column_renamed(col_name, col_name.upper())

                        # Write to Gold Totals Table (Append Mode - or Merge/Delete+Insert)
                        st.write(f"Writing corrected totals to {GOLD_TOTALS_TABLE}...")
                        # Example: Delete existing before insert (use with caution)
                        session.sql(f"DELETE FROM {GOLD_TOTALS_TABLE} WHERE INVOICE_ID = '{selected_invoice_id}'").collect()
                        snowpark_gold_totals.write.mode("append").save_as_table(GOLD_TOTALS_TABLE)
                        st.success(f"Successfully saved corrected totals to {GOLD_TOTALS_TABLE}")

                    # --- Update Silver Tables Review Status ---
                    st.write("Updating review status in Silver tables...")
                    update_query = f"""
                    UPDATE {SILVER_ITEMS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', -- Format for TIMESTAMP_NTZ
                        NOTES = '{review_notes.replace("'", "''")}', -- Escape single quotes
                        CORRECTED_INVOICE_NUMBER = '{corrected_invoice_num.replace("'", "''")}' -- Optional
                    WHERE INVOICE_ID = '{selected_invoice_id}';
                    """
                    session.sql(update_query).collect() # Use collect() to execute the update

                    update_query = f"""
                    UPDATE {SILVER_TOTALS_TABLE}
                    SET REVIEW_STATUS = 'Reviewed',
                        REVIEWED_BY = '{CURRENT_USER}',
                        REVIEWED_TIMESTAMP = '{current_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}',
                        NOTES = '{review_notes.replace("'", "''")}',
                        CORRECTED_INVOICE_NUMBER = '{corrected_invoice_num.replace("'", "''")}' -- Optional
                    WHERE INVOICE_ID = '{selected_invoice_id}';
                    """
                    session.sql(update_query).collect() # Use collect() to execute the update

                    st.success(f"Successfully updated review status for invoice {selected_invoice_id} in Silver tables.")

                    # --- Clear Cache and Rerun ---
                    st.cache_data.clear() # Clear the cache to reflect updated silver status
                    # Clear edited state to avoid accidental resubmission? Maybe not needed with rerun.
                    # if 'edited_transact_items' in st.session_state: del st.session_state.edited_transact_items
                    # if 'edited_transact_totals' in st.session_state: del st.session_state.edited_transact_totals
                    st.info("Refreshing application...")
                    st.rerun() # Rerun the app to refresh the invoice list

                except Exception as e:
                    st.error(f"An error occurred during submission: {e}")

    else:
        st.warning("Please select an invoice ID from the dropdown above.")

else:
    # Only show this message if the list wasn't empty but nothing was selected
    if not invoices_to_review_df.empty:
        st.info("Select an Invoice ID from the dropdown in section 1 to proceed.")
