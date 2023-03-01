CREATE TABLE poc.measurement (
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