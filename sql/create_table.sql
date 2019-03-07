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
  geocoded_state TEXT,
  grid_id INTEGER,
  dom INTEGER,
  dow INTEGER,
  department_category TEXT,
  is_business_district BOOL 
) ;

CREATE TABLE units (
  unit TEXT,
  department_name TEXT,
  department_description TEXT,
  department_category TEXT,
  unit_description TEXT) ;


copy tickets from '/opt/data/tickets/parking-geo.csv' with (FORMAT CSV, DELIMITER ',', NULL '', QUOTE'"', HEADER);
copy units from 'data/unit_key.csv' with (FORMAT CSV, DELIMITER ',', NULL '', QUOTE'"', HEADER);

UPDATE tickets SET department_category = u.department_category 
FROM units u 
WHERE u.unit = tickets.unit ;

UPDATE tickets SET hour = EXTRACT('hour' FROM issue_date) ;
UPDATE tickets SET dow = EXTRACT('dow' FROM issue_date) ;
UPDATE tickets SET dom = EXTRACT('day' FROM issue_date) ;
UPDATE tickets SET penalty = 0 WHERE penalty IS NULL ;
UPDATE tickets SET hearing_disposition = 'No Hearing' WHERE hearing_disposition NOT IN ('Not Liable', 'Liable') ;
UPDATE tickets SET violation_description = initcap(violation_description) ;
