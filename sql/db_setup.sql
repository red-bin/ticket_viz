DROP TABLE TICKETS ;
DROP TABLE UNITS ;

CREATE TABLE tickets (
  ticket_number TEXT,
  issue_date TIMESTAMP,
  violation_location TEXT,
  license_plate_number TEXT,
  license_plate_state TEXT,
  license_plate_type TEXT,
  zipcode TEXT,
  violation_code TEXT,
  violation_description TEXT,
  unit TEXT,
  unit_description TEXT,
  vehicle_make TEXT,
  fine_level1_amount FLOAT,
  fine_level2_amount FLOAT,
  current_amount_due TEXT,
  total_payments FLOAT,
  ticket_queue TEXT,
  ticket_queue_date TEXT,
  notice_level TEXT,
  hearing_disposition TEXT,
  notice_number BIGINT,
  dismissal_reason TEXT,
  officer TEXT,
  address TEXT,
  license_hash TEXT,
  year INTEGER,
  month INTEGER,
  hour INTEGER,
  penalty FLOAT,
  ward INTEGER,
  geocode_accuracy TEXT,
  geocode_accuracy_type TEXT,
  geocoded_address TEXT,
  geocoded_lng FLOAT,
  geocoded_lat FLOAT,
  geocoded_city TEXT,
  geocoded_state TEXT
) ;

CREATE TABLE units (
  unit TEXT,
  department_name TEXT,
  department_description TEXT,
  department_category TEXT,
  unit_description TEXT) ;


copy tickets from '/opt/data/tickets/parking-geo.csv' with (FORMAT CSV, DELIMITER ',', NULL '', QUOTE'"', HEADER);
copy units from '/opt/data/tickets/data/unit_key.csv' with (FORMAT CSV, DELIMITER ',', NULL '', QUOTE'"', HEADER);

ALTER table TICKETS ADD COLUMN grid_id INTEGER ;
ALTER table TICKETS ADD COLUMN dom INTEGER ;
ALTER table TICKETS ADD COLUMN dow INTEGER ;
ALTER table TICKETS ADD COLUMN department_category TEXT ;
ALTER table TICKETS ADD COLUMN is_business_district BOOL ;

--Transfer info from Elliot's dept category FOIA data
UPDATE tickets SET department_category = u.department_category 
  FROM units u 
  WHERE u.unit = tickets.unit ;

--Filling in the blanks
UPDATE tickets SET department_category = 'Unknown' WHERE department_category IS NULL ;
UPDATE tickets SET hearing_disposition = 'No Hearing' WHERE hearing_disposition NOT IN ('Not Liable', 'Liable') ;
UPDATE tickets SET dismissal_reason = 'No Dismissal' WHERE dismissal_reason = '' ;

--making data cleanup easier for later in code
UPDATE tickets SET hour = EXTRACT('hour' FROM issue_date) ;
UPDATE tickets SET dow = EXTRACT('dow' FROM issue_date) ;
UPDATE tickets SET dom = EXTRACT('day' FROM issue_date) ;
UPDATE tickets SET penalty = 0 WHERE penalty IS NULL ;
UPDATE tickets SET violation_description = initcap(violation_description) ;
UPDATE tickets SET dismissal_reason = initcap(dismissal_reason) WHERE dismissal_reason != '';

CREATE index violation_location_idx ON tickets (violation_location);
CREATE index department_category_idx ON tickets (department_category);
CREATE index grid_id_idx ON tickets (grid_id);
CREATE index hearing_disposition_idx ON tickets (hearing_disposition);
CREATE index issue_date_idx ON tickets (issue_date);
CREATE index ticket_queue_idx ON tickets (ticket_queue);
CREATE index geocoded_address_idx ON tickets (geocoded_address);
CREATE INDEX hour_idx ON tickets (hour);
CREATE INDEX dow_idx ON tickets (dow);
CREATE INDEX year_idx ON tickets (year);
CREATE INDEX ward_idx ON tickets (ward);
CREATE INDEX dismissal_reason_idx ON tickets (dismissal_reason);
CREATE INDEX business_district_idx ON tickets (is_business_district);
