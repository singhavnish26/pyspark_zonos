CREATE TABLE poc.dev_event (
device text,
event int,
firstOccurrenceTime timestamp,
lastOccurrenceTime timestamp,
occurrenceCount int,
receiveTime timestamp,
persistTime timestamp,
state text,
PRIMARY KEY (device)
);


CREATE TABLE poc.device_event (
    device text,
    event int,
    receiveTime timestamp,
    firstOccurrenceTime timestamp,
    lastOccurrenceTime timestamp,
    occurrenceCount int,
    persistTime timestamp,
    state text,
    PRIMARY KEY (device)
);