
CREATE DATABASE IF NOT EXISTS cs5424db;
USE cs5424db;

CREATE USER IF NOT EXISTS naili;

-- drop the schema, and reload
DROP SCHEMA IF EXISTS workloadA CASCADE;
CREATE SCHEMA workloadA AUTHORIZATION naili;

GRANT all on DATABASE cs5424db to naili;
GRANT all ON SCHEMA workloadA TO naili;

CREATE TABLE IF NOT EXISTS cs5424db.workloadA.warehouse (
    W_ID INT NOT NULL,
    W_NAME VARCHAR(10) NOT NULL,
    W_STREET_1 VARCHAR(20) NOT NULL,
    W_STREET_2 VARCHAR(20) NOT NULL,
    W_CITY VARCHAR(20) NOT NULL,
    W_STATE CHAR(2) NOT NULL,
    W_ZIP CHAR(9) NOT NULL,
    W_TAX DECIMAL(4,4) NOT NULL,
    W_YTD DECIMAL(12,2) NOT NULL);


IMPORT INTO cs5424db.workloadA.warehouse
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/warehouse.csv')
    WITH delimiter = e',', nullif = 'null';


CREATE TABLE IF NOT EXISTS cs5424db.workloadA.district (
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
    D_NEXT_O_ID INT NOT NULL);


IMPORT INTO cs5424db.workloadA.district
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/district.csv')
    WITH delimiter = e',', nullif = 'null';


CREATE TABLE IF NOT EXISTS cs5424db.workloadA.customer (
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
    C_SINCE TIMESTAMP NOT NULL,
    C_CREDIT CHAR(2) NOT NULL,
    C_CREDIT_LIM DECIMAL(12,2) NOT NULL,
    C_DISCOUNT DECIMAL(4,4) NOT NULL,
    C_BALANCE DECIMAL(12,2) NOT NULL,
    C_YTD_PAYMENT FLOAT NOT NULL,
    C_PAYMENT_CNT INT NOT NULL,
    C_DELIVERY_CNT INT NOT NULL,
    C_DATA VARCHAR(500) NOT NULL);

IMPORT INTO cs5424db.workloadA.customer
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/customer.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadA.order_ori (
    O_W_ID INT,
    O_D_ID INT,
    O_ID INT,
    O_C_ID INT,
    O_CARRIER_ID INT,
    O_OL_CNT DECIMAL(2,0),
    O_ALL_LOCAL DECIMAL(1,0),
    O_ENTRY_D TIMESTAMP);

IMPORT INTO cs5424db.workloadA.order_ori
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/order.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadA.item (
    I_ID INT NOT NULL,
    I_NAME VARCHAR(24) NOT NULL,
    I_PRICE DECIMAL(5,0) NOT NULL,
    I_IM_ID INT NOT NULL,
    I_DATA VARCHAR(50) NOT NULL);

IMPORT INTO cs5424db.workloadA.item
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/item.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadA.order_line (
    OL_W_ID INT NOT NULL,
    OL_D_ID INT NOT NULL,
    OL_O_ID INT NOT NULL,
    OL_NUMBER INT NOT NULL,
    OL_I_ID INT NOT NULL,
    OL_DELIVERY_D TIMESTAMP,
    OL_AMOUNT DECIMAL(6,2) NOT NULL,
    OL_SUPPLY_W_ID INT NOT NULL,
    OL_QUANTITY DECIMAL(2,0) NOT NULL,
    OL_DIST_INFO CHAR(24) NOT NULL);

IMPORT INTO cs5424db.workloadA.order_line
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/order-line.csv')
    WITH delimiter = e',', nullif = 'null';

CREATE TABLE IF NOT EXISTS cs5424db.workloadA.stock (
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
    S_DATA VARCHAR(50) NOT NULL);

IMPORT INTO cs5424db.workloadA.stock
    CSV DATA ('http://localhost:3000/opt/project_files/data_files_A/stock.csv')
    WITH delimiter = e',', nullif = 'null';

GRANT all on TABLE cs5424db.workloadA.* to naili;

set search_path to workloadA;

CREATE INDEX s_joint_id ON stock (s_w_id, s_i_id);
CREATE INDEX c_joint_id ON customer (c_w_id, c_d_id, c_id);
CREATE INDEX d_joint_id ON district (d_w_id, d_id);
CREATE INDEX order_ori_joint_id ON order_ori (o_w_id, o_d_id, o_id, o_carrier_id);
CREATE INDEX order_ori_c_id ON order_ori (o_c_id);
CREATE INDEX order_ori_entry_d ON order_ori (o_entry_d);
CREATE INDEX order_line_joint_id ON order_line (ol_w_id, ol_d_id, ol_o_id, ol_number);
