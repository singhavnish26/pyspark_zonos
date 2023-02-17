CREATE TABLE dev_profile_facts (
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