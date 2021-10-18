CREATE TABLE validation (
    validation_id         SERIAL,
    promocode_type_id     INTEGER NOT NULL,
    promocode             TEXT NOT NULL,
    client_id             INTEGER,
    validation_timestamp  TIMESTAMP
);

CREATE TABLE promocode_type (
    promocode_type_id     SERIAL,
    price                 INTEGER NOT NULL,
    source                TEXT NOT NULL,
    title                 TEXT NOT NULL
);
