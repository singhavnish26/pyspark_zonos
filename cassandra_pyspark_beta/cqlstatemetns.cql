CREATE KEYSPACE poc
WITH replication = {
   'class': 'SimpleStrategy',
   'replication_factor': 1
};

CREATE TABLE emp (
   eid INT PRIMARY KEY,
   ename TEXT,
   sal INT
);


poc.emp