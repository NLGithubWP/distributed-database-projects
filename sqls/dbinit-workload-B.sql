



-- Create db
CREATE DATABASE IF NOT EXISTS cs5424db;
USE cs5424db;


-- set hash index enable to true
SET experimental_enable_hash_sharded_indexes=on;

-- load-splitting threshold to be 400
SET CLUSTER SETTING kv.range_split.by_load_enabled = true;
SET CLUSTER SETTING kv.range_split.load_qps_threshold = 400;

-- Create user
CREATE USER IF NOT EXISTS naili;

-- drop the schema, and reload
DROP SCHEMA IF EXISTS workloadB CASCADE;
CREATE SCHEMA workloadB AUTHORIZATION naili;

-- grant to user
GRANT all on DATABASE cs5424db to naili;
GRANT all ON SCHEMA workloadB TO naili;

-- Create databases and import data
CREATE TABLE IF NOT EXISTS cs5424db.workloadB.warehouse (
    W_ID INT NOT NULL,
    W_NAME VARCHAR(10) NOT NULL,
    W_STREET_1 VARCHAR(20) NOT NULL,
    W_STREET_2 VARCHAR(20) NOT NULL,
    W_CITY VARCHAR(20) NOT NULL,
    W_STATE CHAR(2) NOT NULL,
    W_ZIP CHAR(9) NOT NULL,
    W_TAX DECIMAL(4,4) NOT NULL,
    W_YTD DECIMAL(12,2) NOT NULL,
    FAMILY freqWrite (W_ID, W_YTD),
    FAMILY freqRead (W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX),
    PRIMARY KEY (W_ID));

IMPORT INTO cs5424db.workloadB.warehouse
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/warehouse.csv')
    WITH delimiter = e',', nullif = 'null';


CREATE TABLE IF NOT EXISTS cs5424db.workloadB.district (
    D_W_ID INT NOT NULL,
    D_ID INT NOT NULL,
    D_NAME VARCHAR(10) NOT NULL,
    D_STREET_1 VARCHAR(20) NOT NULL,
    D_STREET_2 VARCHAR(20) NOT NULL,
    D_CITY VARCHAR(20) NOT NULL,
    D_STATE CHAR(2) NOT NULL,
    D_ZIP CHAR(9) NOT NULL,
    D_TAX DECIMAL(4,4) NOT NULL,
    D_YTD DECIMAL(12,2) NOT NULL,
    D_NEXT_O_ID INT NOT NULL,
    FAMILY freqRead (D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX),
    FAMILY freqWrite (D_W_ID, D_ID, D_NEXT_O_ID, D_YTD),
    PRIMARY KEY (D_W_ID, D_ID));


IMPORT INTO cs5424db.workloadB.district
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/district.csv')
    WITH delimiter = e',', nullif = 'null';


CREATE TABLE IF NOT EXISTS cs5424db.workloadB.customer (
    C_W_ID INT NOT NULL,
    C_D_ID INT NOT NULL,
    C_ID INT NOT NULL,
    C_FIRST VARCHAR(16) NOT NULL,
    C_MIDDLE CHAR(2) NOT NULL,
    C_LAST VARCHAR(16) NOT NULL,
    C_STREET_1 VARCHAR(20) NOT NULL,
    C_STREET_2 VARCHAR(20) NOT NULL,
    C_CITY VARCHAR(20) NOT NULL,
    C_STATE CHAR(2) NOT NULL,
    C_ZIP CHAR(9) NOT NULL,
    C_PHONE CHAR(16) NOT NULL,
    C_SINCE TIMESTAMPTZ DEFAULT now() NOT NULL,
    C_CREDIT CHAR(2) NOT NULL,
    C_CREDIT_LIM DECIMAL(12,2) NOT NULL,
    C_DISCOUNT DECIMAL(4,4) NOT NULL,
    C_BALANCE DECIMAL(12,2) NOT NULL,
    C_YTD_PAYMENT FLOAT NOT NULL,
    C_PAYMENT_CNT INT NOT NULL,
    C_DELIVERY_CNT INT NOT NULL,
    C_DATA VARCHAR(500) NOT NULL,
    FAMILY freqWrite (C_W_ID, C_D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT),
    FAMILY freqRead (C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA),
    PRIMARY KEY (C_W_ID, C_D_ID, C_ID));

IMPORT INTO cs5424db.workloadB.customer
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/customer.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadB.order_ori (
    pid UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    O_W_ID INT NOT NULL,
    O_D_ID INT NOT NULL,
    O_ID INT NOT NULL,
    O_C_ID INT NOT NULL,
    O_CARRIER_ID INT,
    O_OL_CNT DECIMAL(2,0) NOT NULL,
    O_ALL_LOCAL DECIMAL(1,0) NOT NULL,
    O_ENTRY_D TIMESTAMPTZ DEFAULT now(),
    FAMILY freqWrite (pid, O_W_ID, O_D_ID, O_ID, O_CARRIER_ID),
    FAMILY freqRead (O_C_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D),
    INDEX order_ori_joint_id (o_w_id, o_d_id, o_id, o_carrier_id),
    INDEX order_ori_joint_c_id (o_w_id, o_d_id, o_c_id),
    INDEX order_ori_c_id (o_c_id),
    INDEX order_ori_entry_d (o_entry_d));

IMPORT INTO cs5424db.workloadB.order_ori (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/order.csv')
    WITH delimiter = e',', nullif = 'null';


CREATE TABLE IF NOT EXISTS cs5424db.workloadB.item (
    I_ID INT NOT NULL,
    I_NAME VARCHAR(24) NOT NULL,
    I_PRICE DECIMAL(5,0) NOT NULL,
    I_IM_ID INT NOT NULL,
    I_DATA VARCHAR(50) NOT NULL,
    PRIMARY KEY (I_ID));


IMPORT INTO cs5424db.workloadB.item (I_ID,I_NAME,I_PRICE,I_IM_ID,I_DATA)
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/item.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadB.order_line (
    pid UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    OL_W_ID INT NOT NULL,
    OL_D_ID INT NOT NULL,
    OL_O_ID INT NOT NULL,
    OL_NUMBER INT NOT NULL,
    OL_I_ID INT NOT NULL,
    OL_DELIVERY_D TIMESTAMPTZ DEFAULT now(),
    OL_AMOUNT DECIMAL(6,2) NOT NULL,
    OL_SUPPLY_W_ID INT NOT NULL,
    OL_QUANTITY DECIMAL(2,0) NOT NULL,
    OL_DIST_INFO CHAR(24) NOT NULL,
    FAMILY freqRead (OL_I_ID, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO),
    FAMILY freqWrite (pid, ol_w_id, ol_d_id, ol_o_id, ol_number, OL_DELIVERY_D),
    INDEX order_line_joint_id (ol_w_id, ol_d_id, ol_o_id, ol_number));

IMPORT INTO cs5424db.workloadB.order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/order-line.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadB.stock (
    S_W_ID INT NOT NULL,
    S_I_ID INT NOT NULL,
    S_QUANTITY DECIMAL(4,0) NOT NULL,
    S_YTD DECIMAL(8,2) NOT NULL,
    S_ORDER_CNT INT NOT NULL,
    S_REMOTE_CNT INT NOT NULL,
    S_DIST_01 CHAR(24) NOT NULL,
    S_DIST_02 CHAR(24) NOT NULL,
    S_DIST_03 CHAR(24) NOT NULL,
    S_DIST_04 CHAR(24) NOT NULL,
    S_DIST_05 CHAR(24) NOT NULL,
    S_DIST_06 CHAR(24) NOT NULL,
    S_DIST_07 CHAR(24) NOT NULL,
    S_DIST_08 CHAR(24) NOT NULL,
    S_DIST_09 CHAR(24) NOT NULL,
    S_DIST_10 CHAR(24) NOT NULL,
    S_DATA VARCHAR(50) NOT NULL,
    FAMILY freqRead (S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07,
        S_DIST_08, S_DIST_09, S_DIST_10, S_DATA),
    FAMILY freqWrite (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT),
    PRIMARY KEY (S_W_ID, S_I_ID));

IMPORT INTO cs5424db.workloadB.stock
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/stock.csv')
    WITH delimiter = e',', nullif = 'null';

-- grant to user
GRANT all on TABLE cs5424db.workloadB.* to naili;

-- set schema
set search_path to workloadB;


-- run some test sql
select * from item limit 100;
SHOW INDEX FROM item;
SHOW COLUMNS FROM item;
show ranges from table item;


-- de-normalization to make read faster

-- de-normalization order_ori
ALTER TABLE cs5424db.workloadB.order_ori
    ADD COLUMN O_C_FIRST VARCHAR(16) FAMILY freqRead,
    ADD COLUMN O_C_MIDDLE CHAR(2) FAMILY freqRead,
    ADD COLUMN O_C_LAST VARCHAR(16) FAMILY freqRead;

UPDATE order_ori SET O_C_FIRST = C_FIRST, O_C_MIDDLE=C_MIDDLE, O_C_LAST=C_LAST
    FROM customer
    WHERE order_ori.O_W_ID=customer.C_W_ID AND order_ori.O_D_ID=customer.C_D_ID AND order_ori.O_C_ID=customer.C_ID;

-- de-normalization customer
ALTER TABLE cs5424db.workloadB.customer
    ADD COLUMN C_W_NAME VARCHAR(10) FAMILY freqRead,
    ADD COLUMN C_D_NAME VARCHAR(10) FAMILY freqRead;


WITH dw AS
    (select D_W_ID, D_ID, W_NAME, D_NAME from district left join warehouse on district.D_W_ID = warehouse.W_ID)
UPDATE customer SET C_W_NAME = W_NAME, C_D_NAME = D_NAME
    FROM dw WHERE customer.C_W_ID = dw.D_W_ID AND customer.C_D_ID = dw.D_ID;

-- de-normalization order_line
ALTER TABLE cs5424db.workloadB.order_line ADD COLUMN OL_I_NAME VARCHAR(24) FAMILY freqRead;

UPDATE order_line SET OL_I_NAME = I_NAME
    FROM item
    WHERE order_line.OL_I_ID=item.I_ID;
