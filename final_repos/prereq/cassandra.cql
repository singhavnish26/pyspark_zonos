CREATE KEYSPACE reporting WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE reporting.dev_event (
    device text,
    event int,
    receiveTime timestamp,
    firstOccurrenceTime timestamp,
    lastOccurrenceTime timestamp,
    occurrenceCount int,
    persistTime timestamp,
    state text,
    PRIMARY KEY (device, event)
);
CREATE TABLE reporting.dev_measurement (
    device text,
    measuretime text,
    receivetime text,
    persisttime text,
    readingreason text,
    register text,
    value float,
    unit text,
    profile text,
    PRIMARY KEY (device, register)
);
CREATE TABLE reporting.dev_parameter (
    device text,
    parameter text,
    changetime timestamp,
    receivetime timestamp,
    persisttime timestamp,
    newvalue text,
    oldvalue text,
    PRIMARY KEY (device, parameter, changetime)
);
CREATE TABLE reporting.dev_process (
  id INT,
  type TEXT,
  device TEXT,
  persisttime TIMESTAMP,
  inittime TIMESTAMP,
  starttime TIMESTAMP,
  previous_status TEXT,
  status TEXT,
  error TEXT,
  completiontime TIMESTAMP,
  executiontype TEXT,
  profile_id TEXT,
  reading_reason_code TEXT,
  PRIMARY KEY (id)
);
CREATE TABLE reporting.dev_profile_facts (
  device text,
  profile text,
  gatheredAt timestamp,
  actual double,
  expected double,
  completeness text,
  from_time timestamp,
  to_time timestamp,
  PRIMARY KEY ((device, profile), gatheredAt)
);
CREATE TABLE reporting.dev_telemetry (
  device text,
  lastreceivetime timestamp,
  PRIMARY KEY (device)
);
CREATE TABLE reporting.device (
   persistTime TIMESTAMP,
   device TEXT,
   type TEXT,
   group TEXT,
   inventorystate TEXT,
   managementstate TEXT,
   communicationid TEXT,
   manufacturer TEXT,
   description TEXT,
   model TEXT,
   latitude DOUBLE,
   longitude DOUBLE,
   city TEXT,
   postalcode TEXT,
   street TEXT,
   housenumber TEXT,
   floor TEXT,
   company TEXT,
   country TEXT,
   reference TEXT,
   timezone TEXT,
   region TEXT,
   district TEXT,
   logicalinstallationpoint TEXT,
   PRIMARY KEY (manufacturer)
);
CREATE TABLE reporting.dev_operational_process (
  id INT,
  type TEXT,
  persisttime TIMESTAMP,
  status TEXT,
  PRIMARY KEY (id)
);
CREATE TABLE reporting.register_statistics (
    device TEXT,
    register TEXT,
    date TEXT,
    mr_expected INT,
    mr_received INT,
    mr_edited INT,
    mr_invalidated INT,
    mr_estimated INT,
    dd_minimum INT,
    dd_maximum INT,
    dd_average FLOAT,
    PRIMARY KEY (device, register, date)
);

