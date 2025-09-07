CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'd8TySvmWr4rPBUk';
CREATE DATABASE SCOPED CREDENTIAL farizadmin WITH IDENTITY = 'Managed Identity';

select * from sys.database_credentials

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://bigdatastorageaccount.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = farizadmin
);


CREATE EXTERNAL TABLE gold.finaltables WITH (
        LOCATION = 'Serving',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final;

DROP EXTERNAL TABLE gold.finaltables;


