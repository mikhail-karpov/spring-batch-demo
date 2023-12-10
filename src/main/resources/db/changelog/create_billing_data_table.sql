--liquibase formatted sql
--changeset karpovmi:2

create table BILLING_DATA (
    DATA_YEAR     INTEGER,
    DATA_MONTH    INTEGER,
    ACCOUNT_ID    INTEGER,
    PHONE_NUMBER  VARCHAR(12),
    DATA_USAGE    FLOAT,
    CALL_DURATION INTEGER,
    SMS_COUNT     INTEGER
);
