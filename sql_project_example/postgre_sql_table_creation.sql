-- CREATE EACH TABLES AND IMPORT CSV FILE TO NEW TABLES
-- TO IMPORT CSV FILES : RIGHT CLICK NEW TABLE -- IMPORT/EXPORT -- CHOOSE IMPORT--SELECT DOCUMENT DIRECTORY -- CSV HEADER +DELIMITER (,)
-- 1. COMPLETEDACCT TABLE 
CREATE TABLE COMPLETEDACCT (
account_id varchar(255) unique not null, 
district_id int, 
frequency varchar(255), 
parseddate date, 
year int, 
month int, 
day int);


--2. COMPLETEDCARD TABLE 
CREATE TABLE COMPLETEDCARD(
card_id	VARCHAR(255) UNIQUE NOT NULL, 
disp_id	VARCHAR(255), 
type VARCHAR(255),
year INT, 
month INT,
day INT,
fulldate DATE);

--3. COMPLETEDCLIENT TABLE
CREATE TABLE COMPLETEDCLIENT( 
client_id VARCHAR(255) UNIQUE NOT NULL, 
sex VARCHAR(255),
fulldate DATE, 
day	INT, 
month INT, 	
year INT, 
age INT,
social VARCHAR(255),
first VARCHAR(255),
middle VARCHAR(255), 
last VARCHAR(255),
phone VARCHAR(255),
email VARCHAR(255),
address_1 VARCHAR (255),
address_2 VARCHAR(255),
city VARCHAR(255), 
state VARCHAR(255),
zipcode INT,
district_id INT);

-- 4. COMPLETEDDISPOSTION TABLE 
CREATE TABLE COMPLETEDDISPOSITION (
disp_id VARCHAR (255) UNIQUE NOT NULL,
client_id VARCHAR(255) , 
account_id VARCHAR(255), 
type VARCHAR(255));

-- 5. COMPLETEDDISTRICT TABLE 
CREATE TABLE COMPLETEDDISTRICT(
district_id INT UNIQUE NOT NULL, 
city VARCHAR(255),
state_name VARCHAR(255),
state_abbrev VARCHAR(255),
region VARCHAR(255),
division VARCHAR(255))

-- 6. COMPLETEDLOAN TABLE 
CREATE TABLE COMPLETEDLOAN(
loan_id VARCHAR(255) UNIQUE NOT NULL,
account_id VARCHAR(255),
amount INT,
duration INT,
payments FLOAT,
status VARCHAR (255),
year INT,
month INT,
day INT,
fulldate DATE,
location INT,
purpose VARCHAR(255))

-- 7. COMPLETEDORDER TABLE 
CREATE TABLE COMPLETEDORDER (
order_id INT NOT NULL, 
account_id VARCHAR(255),
bank_to	VARCHAR(255),
account_to INT, 
amount FLOAT, 
k_symbol VARCHAR(255))

-- 8. COMPLETEDTRANS TABLE 
CREATE TABLE COMPLETEDTRANS(
number int,
trans_id VARCHAR(255),
account_id VARCHAR(255),
type VARCHAR(255),
operation VARCHAR(255),
amount FLOAT, 
balance	FLOAT,
k_symbol VARCHAR(255),
bank VARCHAR(255),
account VARCHAR(255),
year INT, 
month INT,
day INT,
fulldate DATE,
fulltime VARCHAR(255), 
fulldatewithtime VARCHAR(255));

-- 9. CRM CALL CENTER LOGS TABLE
CREATE TABLE CRMCALLCENTERLOGS(
Date_received DATE, 
Complaint_ID VARCHAR(255),
rand_client VARCHAR(255),
phonefinal VARCHAR(255),
vru_line VARCHAR(255),
call_id INT,
priority INT,
type VARCHAR(255),
outcome	VARCHAR(255),
server VARCHAR(255),
ser_start TIME, 
ser_exit TIME,
ser_time TIME)

-- 10. CRM EVENTS TABLE 
DROP TABLE CRMEVENTSTABLE; 
CREATE TABLE CRMEVENTSTABLE(
Date_received VARCHAR(255), 
Product VARCHAR(255),
Sub_product VARCHAR(255),
Issue VARCHAR(255),
Sub_issue VARCHAR(255),
Consumer_complaint_narrative TEXT,
Tags VARCHAR(255),
Consumer_consent_provided VARCHAR(255),
Submitted_via VARCHAR(255),
Date_Sent_to_company VARCHAR(255), 
Company_response_to_consumer VARCHAR(255),
Timely_response VARCHAR(255),
Consumer_disputed VARCHAR(255),
Complaint_ID VARCHAR(255),
Client_ID VARCHAR(255))

-- 11. CRM REVIEWS TABLE 
CREATE TABLE CRM_REVIEWS(
Date VARCHAR(255) NULL,
Stars VARCHAR(255)NULL,
Reviews TEXT NULL,
Product VARCHAR(255) NULL,
district_id VARCHAR(255)NULL);

-- 12. LUXURY LOAN PORTFOLIO TABLE 
CREATE TABLE LUXURY_LOAN(
loan_id VARCHAR(255),
funded_amount FLOAT,
funded_date DATE,
duration_years INT, 
duration_months INT,http://127.0.0.1:63662/datagrid/panel/6266780?is_query_tool=true&sgid=1&sid=2&server_type=pg&did=14021#
ten_yr_treasury_index_date_funded FLOAT, 
interest_rate_percent FLOAT, 
interest_rate FLOAT,
payments FLOAT,
total_past_payments FLOAT, 
loan_balance FLOAT, 
property_value FLOAT, 
purpose	VARCHAR(255),
firstname VARCHAR(255),
middlename VARCHAR(255),
lastname VARCHAR(255),
social VARCHAR(255),
phone VARCHAR(255),
title VARCHAR(255),
employment_length INT, 
BUILDING_CLASS_CATEGORY	VARCHAR(255),
TAX_CLASS_AT_PRESENT VARCHAR(255),
BUILDING_CLASS_AT_PRESENT VARCHAR(255),
ADDRESS_1 VARCHAR(255),
ADDRESS_2 VARCHAR(255),
ZIP_CODE INT, 
CITY VARCHAR(255),
STATE VARCHAR(255),
TOTAL_UNITS FLOAT, 
LAND_SQUARE_FEET VARCHAR(255), 
GROSS_SQUARE_FEET VARCHAR(255), 
TAX_CLASS_AT_TIME_OF_SALE FLOAT);

---- RUN THE LOGIC 
--- JOIN TABLE
DROP TABLE JOIN_COMPLAINT; 
SELECT A.DATE_RECEIVED, A.COMPLAINT_ID, 
A.RAND_CLIENT, A.PHONEFINAL, A.VRU_LINE, A.CALL_ID, A.PRIORITY, 
A.TYPE, A.OUTCOME, A.SERVER, A.SER_START, A.SER_EXIT, A.SER_TIME,
B.DATE_RECEIVED AS DATE2, B.PRODUCT, B.SUB_PRODUCT, B.ISSUE, B.SUB_ISSUE, 
B.CONSUMER_COMPLAINT_NARRATIVE, B.TAGS, B.CONSUMER_CONSENT_PROVIDED, 
B.SUBMITTED_VIA, B.DATE_SENT_TO_COMPANY, 
B.COMPANY_RESPONSE_TO_CONSUMER, B.TIMELY_RESPONSE, B.CONSUMER_DISPUTED, 
B.CLIENT_ID 
INTO JOIN_COMPLAINT 
FROM CRMCALLCENTERLOGS A LEFT JOIN CRMEVENTSTABLE B ON A.COMPLAINT_ID = B.COMPLAINT_ID
WHERE A.COMPLAINT_ID IS NOT NULL; 

---CHANGE DATA SER_TIME TO MINUTE
ALTER TABLE JOIN_COMPLAINT ADD MINUTE_SER FLOAT 
ALTER TABLE JOIN_COMPLAINT ADD HOUR FLOAT, ADD MINUTE FLOAT, ADD SECOND FLOAT; 

UPDATE JOIN_COMPLAINT
SET HOUR =EXTRACT(HOUR FROM SER_TIME), MINUTE = EXTRACT(MINUTE FROM SER_TIME), SECOND = EXTRACT(SECOND FROM SER_TIME)

UPDATE JOIN_COMPLAINT 
SET MINUTE_SER = (HOUR*60)+(MINUTE)+(SECOND/60)

SELECT TYPE, AVG(MINUTE_SER) FROM JOIN_COMPLAINT GROUP BY TYPE; 


---SUMMARIZE AVERAGETIME 
--1.DATE_RECEIVED 
DROP TABLE AVG_DATE;
SELECT LEFT(CAST(DATE_RECEIVED AS VARCHAR),7) AS DATE, AVG(MINUTE_SER) AS TIME 
INTO AVG_DATE FROM JOIN_COMPLAINT GROUP BY DATE; 

--2.VRU_LINE
DROP TABLE AVG_VRU; 
SELECT VRU_LINE, AVG(MINUTE_SER) AS TIME 
INTO AVG_VRU FROM JOIN_COMPLAINT GROUP BY VRU_LINE; 

--3.PRIORITY
DROP TABLE AVG_PRIORITY; 
SELECT PRIORITY, AVG(MINUTE_SER) AS TIME 
INTO AVG_PRIORITY FROM JOIN_COMPLAINT GROUP BY PRIORITY; 

--4.TYPE
DROP TABLE AVG_TYPE; 
SELECT TYPE, AVG(MINUTE_SER) AS TIME 
INTO AVG_TYPE FROM JOIN_COMPLAINT GROUP BY TYPE;

--5.SERVER
DROP TABLE AVG_SERVER
SELECT SERVER,AVG(MINUTE_SER) AS TIME
INTO AVG_SERVER FROM JOIN_COMPLAINT GROUP BY SERVER; 

--6.PRODUCT
DROP TABLE AVG_PRODUCT 
SELECT PRODUCT,AVG(MINUTE_SER) AS TIME 
INTO AVG_PRODUCT FROM JOIN_COMPLAINT GROUP BY PRODUCT; 

--7.SUB_PRODUCT
DROP TABLE AVG_SUB_PRODUCT
SELECT SUB_PRODUCT, AVG(MINUTE_SER) AS TIME 
INTO AVG_SUB_PRODUCT FROM JOIN_COMPLAINT GROUP BY SUB_PRODUCT; 

--8.ISSUE
DROP TABLE AVG_ISSUE 
SELECT ISSUE, AVG(MINUTE_SER) AS TIME 
INTO AVG_ISSUE FROM JOIN_COMPLAINT GROUP BY ISSUE; 

--9.TAGS
DROP TABLE AVG_TAGS 
SELECT TAGS, AVG(MINUTE_SER) AS TIME 
INTO AVG_TAGS FROM JOIN_COMPLAINT GROUP BY TAGS; 

--10.RESPONSE
DROP TABLE AVG_RESPONSE 
SELECT COMPANY_RESPONSE_TO_CONSUMER, AVG(MINUTE_SER) AS TIME 
INTO AVG_RESPONSE FROM JOIN_COMPLAINT GROUP BY COMPANY_RESPONSE_TO_CONSUMER; 

--11.TIMELY_RESPONSE
DROP TABLE AVG_TIMELYRES
SELECT TIMELY_RESPONSE, AVG(MINUTE_SER) AS TIME 
INTO AVG_TIMELYRES FROM JOIN_COMPLAINT GROUP BY TIMELY_RESPONSE; 

--12.CONSUMER_DISPUTED
DROP TABLE AVG_CONSDIP
SELECT CONSUMER_DISPUTED, AVG(MINUTE_SER) AS TIME 
INTO AVG_CONSDIP FROM JOIN_COMPLAINT GROUP BY CONSUMER_DISPUTED; 