USE ROLE ACCOUNTADMIN;

-- CREATE A DOC AI ROLE TO BE USED FOR THE QUICKSTART
CREATE ROLE doc_ai_qs_role;
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_qs_role;

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
CREATE STAGE doc_ai_qs_db.doc_ai_schema.doc_ai_stage
  DIRECTORY = (enable = true)
  ENCRYPTION = (type = 'snowflake_sse');

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
    quantity INTEGER,
    unit_price NUMBER(10, 2),
    total_price NUMBER(12, 2)
);

-- Example INSERT statement for the first few rows
INSERT INTO doc_ai_qs_db.doc_ai_schema.TRANSACT_ITEMS (invoice_id, product_name, quantity, unit_price, total_price) VALUES
    ('58771918316','MODERE LEAN BODY COCONUT LIME GO FRUIT PUNCH', 10, 125.00, 1250.00),
    ('58771918316','MODERE LEAN BODY COCONUT LIME GO AÇAI POMEGRANATE', 10, 125.00, 1250.00),
    ('58771918316','MODERE LEAN BODY CHOCOLATE GO FRUIT PUNCH', 10, 125.00, 1250.00),
    ('58771918316','MODERE RECOVER 20 CT (6g EACH)', 10, 17.00, 170.00),
    ('58771918316','MODERE CELLPROOF LIP COMPLEX (5CT)', 10, 99.00, 990.00),
    ('58771918316','MODERE LEAN BODY SYSTEM VANILLA', 10, 78.00, 780.00),
    ('58771918316','MODERE LEAN BODY SYSTEM CHOCOLATE', 2, 78.00, 156.00), -- This row will have intentionally modified values
    ('58771918316','MODERE LEAN BODY SYSTEM LEMON', 10, 78.00, 780.00),
    ('58771918316','MODERE LEAN BODY SYSTEM COCONUT LIME', 10, 78.00, 780.00),
    ('58771918316','MODERE TRIM VANILLA 450 ML', 10, 59.00, 590.00),
    ('58771918316','MODERE TRIM CHOCOLATE 450 ML', 5, 100.00, 500.00), -- This row will have intentionally modified values
    ('58771918316','MODERE TRIM LEMON 450 ML', 10, 59.00, 590.00),
    ('58771918316','MODERE LIQUID BIOCELL SKIN (2CT)', 10, 73.00, 730.00),
    ('58771918316','MODERE LIQUID BIOCELL LIFE (2CT)', 10, 73.00, 730.00),
    ('58771918316','MODERE CELLPROOF DUO', 10, 89.00, 890.00),
    ('58771918316','MODERE BURN', 10, 34.00, 340.00),
    ('INV-0209','President Ribbon', 9, 54.02, 486.16),
    ('INV-0209','Vice President Ribbon', 2, 54.02, 108.04), -- This row will have intentionally modified values
    ('INV-0209','Secetary Tresurer Ribbon', 9, 54.02, 486.16),
    ('INV-0209','Mentor Co-ordinator Ribbon', 9, 54.02, 486.16),
    ('INV-0209','Network Education Coordinator', 9, 54.02, 486.16),
    ('INV-0209','Chapter Growth Ribbon', 9, 54.02, 486.16),
    ('INV-0209','Event Co-ordinator Ribbon', 9, 54.02, 486.16),
    ('INV-0209','Membership Committee', 23, 54.02, 1242.41),
    ('INV-0209','Visitor Host Ribbon', 27, 54.02, 1458.48),
    ('INV-0209','Rainbow Rabbit Ribbon', 17, 54.02, 918.30), -- This row will have intentionally modified values
    ('INV-0209','Gold Club Member Pin 2 Stone(M)', 2, 245.54, 491.07),
    ('02009024','AMD RYZEN 5 3400G', 1, 225.00, 225.00),
    ('02009024','MSI B450M BAZOOKA MAX', 1, 168.00, 168.00),
    ('02009024','F4-3200C16D-16GFX', 1, 119.00, 119.00),
    ('02009024','CM MASTERWATT 550W 80+', 1, 89.00, 89.00),
    ('02009024','CM MASTERBOX MB320L', 1, 89.00, 89.00),
    ('02009024','SAMSUNG 860 EVO 250GB M.2', 1, 81.00, 81.00),
    ('02009024','TOSHIBA P300 2TB', 1, 83.00, 83.00);
    
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
    ('58771918316','2020-10-21', 12746.00, 878.47, 12912.27),
    ('INV-0209','2022-03-18', 7135.26, 856.23, 7991.49),
    ('02009024','2020-09-20',854.00, 0, 854.00);
    
 SELECT * FROM doc_ai_qs_db.doc_ai_schema.TRANSACT_TOTALS;