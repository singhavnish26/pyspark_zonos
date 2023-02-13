CREATE TABLE ext_device_events (
  device text,
  event text,
  first_occurrence_time timestamp,
  last_occurrence_time timestamp,
  occurrence_count int,
  receive_time timestamp,
  persist_time timestamp,
  state text,
  context text,
  PRIMARY KEY (device, event)
);
