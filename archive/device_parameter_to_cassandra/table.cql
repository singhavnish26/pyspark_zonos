root
 |-- device: string (nullable = true)
 |-- parameter: string (nullable = true)
 |-- changetime: timestamp (nullable = true)
 |-- receivetime: timestamp (nullable = true)
 |-- persisttime: timestamp (nullable = true)
 |-- newvalue: string (nullable = true)
 |-- oldvalue: string (nullable = true)

CREATE TABLE dev_parameter (
    device text,
    parameter text,
    changetime timestamp,
    receivetime timestamp,
    persisttime timestamp,
    newvalue text,
    oldvalue text,
    PRIMARY KEY (device, parameter, changetime)
);
