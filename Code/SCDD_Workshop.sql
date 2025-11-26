-- create a demo db/schema: DEMO_NAME_SDCC; 
CREATE DATABASE IF NOT EXISTS DEMO_{NAME}_SDCC;

USE DATABASE  DEMO_{NAME}_SDCC;
USE SCHEMA PUBLIC;

-- STEP 1:
-- we first look at the source database and the csv file
select * from swiss_geographic_basics.swiss_geo_basics.buildings
limit 20;

-- add file through plus sign and load it into a table called "students"
select * from students;


/*
-- create a table to load the students file information. 
CREATE OR REPLACE TABLE students ( --since we're inside a shared read-only database, I have to create the table within the database and address fully the workspace database
building_id INT, 
student_name varchar(50),
allergies varchar(100)
);

COPY INTO students 
FROM @demo_workshop_sdcc.public.data/building_residents_allergies.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

*/


-- STEP 2: 
-- create a view to select only residential and school building in canton Zoug
CREATE OR REPLACE VIEW buildings_zg AS
SELECT *,
SPLIT_PART(ZIP_LOCALITY, ' ', 1) AS ZIP, --we create 2 extra columns for future use
SPLIT_PART(ZIP_LOCALITY, ' ', 2) AS LOCALITY
FROM swiss_geographic_basics.swiss_geo_basics.buildings
WHERE official = TRUE
AND canton = 'ZG'
AND (building_category = 'residential' OR building_name ILIKE '%Schul%');

select * from buildings_zg;


select * from swiss_geographic_basics.swiss_geo_basics.streets;

CREATE OR REPLACE VIEW streets_zg AS
SELECT *
FROM swiss_geographic_basics.swiss_geo_basics.streets
WHERE canton = 'ZG';
select * from streets_zg;

-- let's do a join to add the information about the municipality from the table streets 
CREATE OR REPLACE VIEW buildings_with_municipality_zg AS
SELECT DISTINCT
    b.building_category,
    b.building_name,
    b.building_id,
    b.street,
    b.ZIP_LOCALITY,
    b.CANTON AS BUILDING_CANTON,
    b.MUNICIPALITY_NR,
    b.latitude,
    b.longitude,
    s.MUNICIPALITY,
    b.POINT_WGS84
FROM buildings_zg b
JOIN streets_zg s
  ON b.street_id = s.street_id;

select * from buildings_with_municipality_zg; 

-- let's separate all the residential buildings from the schools 
CREATE OR REPLACE VIEW residences_zg AS
SELECT *
FROM buildings_with_municipality_zg
WHERE building_category = 'residential';

select * from residences_zg;
--limit 1000; 

-- let's join the residences table with the file table to mantain only the residents of the students we have collected. 
CREATE OR REPLACE TABLE buildings_zg_students AS
SELECT
r.*,
s.name as student_name, 
s.food_allergies as allergies
FROM residences_zg r 
JOIN students s 
ON r.building_id = s.building_id; 

select * from buildings_zg_students; 

-- let's separate the schools 
CREATE OR REPLACE VIEW schools_zg AS
SELECT *
FROM buildings_with_municipality_zg
WHERE building_name ILIKE '%Schul%'; 
select * from public.schools_zg;


-- STEP 3: 
-- we now create a new table with the average distance of each residence to the nearest school keeping the info from the students table. 
CREATE OR REPLACE TABLE residence_school_mart_zg AS
SELECT 
    r.BUILDING_ID AS residence_id,
    r.STREET,
    r.student_name,
    r.allergies,
    r.municipality,
    r.ZIP_LOCALITY,
    r.BUILDING_CANTON,
    s.BUILDING_ID AS school_id,
    s.BUILDING_NAME AS school_name,
    ST_DISTANCE(r.POINT_WGS84, s.POINT_WGS84) AS distance_meters--compute geospacial distance between school and residence in meters
FROM buildings_zg_students r
JOIN schools_zg s
    ON ABS(r.LATITUDE - s.LATITUDE) < 0.1
    AND ABS(r.LONGITUDE - s.LONGITUDE) < 0.1
QUALIFY ROW_NUMBER() OVER (   --assigns a ranking to each residence-school pair 
    PARTITION BY r.BUILDING_ID -- restart numbering for each residence 
    ORDER BY ST_DISTANCE(r.POINT_WGS84, s.POINT_WGS84)  -- order schools by their distance from that residence 
) = 1;  -- keep only the nearest school for each residence . --> we should get one residence + its closest school + distance in meters 

select * from residence_school_mart_zg;

SELECT 
    MUNICIPALITY,
    COUNT(*) AS num_residences, -- counting the number of rows per group=municipality 
    AVG(distance_meters) AS avg_distance, 
    MEDIAN(distance_meters) AS median_distance,
    MAX(distance_meters) AS max_distance
FROM residence_school_mart_zg
GROUP BY MUNICIPALITY
ORDER BY avg_distance DESC
LIMIT 20;


-- STEP 4: 
-- now we translate the the allergies in swiss languages usign snowflakes' LLM: Cortex
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';

CREATE OR REPLACE TABLE residence_school_mart_zg_translated AS
SELECT
    *,
    SNOWFLAKE.CORTEX.TRANSLATE(m.allergies, 'en', 'fr') AS allergies_french,
    SNOWFLAKE.CORTEX.TRANSLATE(m.allergies, 'en', 'de') AS allergies_german,
    SNOWFLAKE.CORTEX.TRANSLATE(m.allergies, 'en', 'it') AS allergies_italian
FROM residence_school_mart_zg m;

select * from residence_school_mart_zg_translated;


CREATE OR REPLACE CORTEX SEARCH SERVICE RESIDENCE_SCHOOL_CHATBOT_SEARCH
    ON allergies_text
    ATTRIBUTES RESIDENCE_ID, STREET, STUDENT_NAME, MUNICIPALITY, ZIP_LOCALITY, BUILDING_CANTON, SCHOOL_ID, SCHOOL_NAME, DISTANCE_METERS, ALLERGIES_GERMAN, ALLERGIES_FRENCH, ALLERGIES_ITALIAN
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT 
            RESIDENCE_ID,
            TO_VARCHAR(ALLERGIES) AS allergies_text,  -- Convert to VARCHAR if needed
            STREET,
            STUDENT_NAME,
            MUNICIPALITY,
            ZIP_LOCALITY,
            BUILDING_CANTON,
            SCHOOL_ID,
            SCHOOL_NAME,
            ALLERGIES_FRENCH,
            ALLERGIES_GERMAN,
            ALLERGIES_ITALIAN,
            DISTANCE_METERS
        FROM residence_school_mart_zg_translated
    );

GRANT USAGE ON CORTEX SEARCH SERVICE RESIDENCE_SCHOOL_CHATBOT_SEARCH TO ROLE ACCOUNTADMIN;
select * from residence_school_mart_zg_translated