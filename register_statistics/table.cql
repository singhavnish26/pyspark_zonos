CREATE TABLE reg_stat (
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
