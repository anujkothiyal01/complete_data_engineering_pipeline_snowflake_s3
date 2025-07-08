CREATE OR REPLACE DATABASE ecommerce_pipeline;

CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA gold;

USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE customers (
    customer_id INTEGER,
    customer_name STRING,
    email STRING,
    country STRING
);

CREATE OR REPLACE TABLE order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DOUBLE
);

CREATE OR REPLACE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date TIMESTAMP,
    payment_method STRING
);

CREATE OR REPLACE TABLE payments (
    payment_id INTEGER,
    order_id INTEGER,
    amount DOUBLE,
    payment_status STRING
);

CREATE OR REPLACE TABLE products (
    product_id INTEGER,
    product_name STRING,
    category STRING,
    price DOUBLE
);

CREATE OR REPLACE FILE FORMAT ecommerce_csv_format
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STORAGE INTEGRATION ecommerce_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::478024548952:role/ecommerce-pipeline-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://ecommerce-pipeline-bucket01/');

CREATE OR REPLACE STAGE s3_stage_customers
URL = 's3://ecommerce-pipeline-bucket01/customers/'
STORAGE_INTEGRATION = ecommerce_s3_integration
FILE_FORMAT = ecommerce_csv_format;

CREATE OR REPLACE STAGE s3_stage_order_items
URL = 's3://ecommerce-pipeline-bucket01/order_items/'
STORAGE_INTEGRATION = ecommerce_s3_integration
FILE_FORMAT = ecommerce_csv_format;

CREATE OR REPLACE STAGE s3_stage_orders
URL = 's3://ecommerce-pipeline-bucket01/orders/'
STORAGE_INTEGRATION = ecommerce_s3_integration
FILE_FORMAT = ecommerce_csv_format;

CREATE OR REPLACE STAGE s3_stage_payments
URL = 's3://ecommerce-pipeline-bucket01/payments/'
STORAGE_INTEGRATION = ecommerce_s3_integration
FILE_FORMAT = ecommerce_csv_format;

CREATE OR REPLACE STAGE s3_stage_products
URL = 's3://ecommerce-pipeline-bucket01/products/'
STORAGE_INTEGRATION = ecommerce_s3_integration
FILE_FORMAT = ecommerce_csv_format;

DESC INTEGRATION ecommerce_s3_integration;

CREATE OR REPLACE PIPE pipe_customers
AUTO_INGEST = TRUE
AS
COPY INTO customers
FROM @s3_stage_customers
FILE_FORMAT = (FORMAT_NAME = ecommerce_csv_format),
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE pipe_order_items
AUTO_INGEST = TRUE
AS
COPY INTO order_items
FROM @s3_stage_order_items
FILE_FORMAT = (FORMAT_NAME = ecommerce_csv_format),
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE pipe_orders
AUTO_INGEST = TRUE
AS
COPY INTO orders
FROM @s3_stage_orders
FILE_FORMAT = (FORMAT_NAME = ecommerce_csv_format),
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE pipe_payments
AUTO_INGEST = TRUE
AS
COPY INTO payments
FROM @s3_stage_payments
FILE_FORMAT = (FORMAT_NAME = ecommerce_csv_format),
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE pipe_products
AUTO_INGEST = TRUE
AS
COPY INTO products
FROM @s3_stage_products
FILE_FORMAT = (FORMAT_NAME = ecommerce_csv_format),
ON_ERROR = 'CONTINUE';

DESC PIPE pipe_customers;
DESC PIPE pipe_order_items;
DESC PIPE pipe_orders;
DESC PIPE pipe_payments;
DESC PIPE pipe_products;

SELECT * FROM customers;
SELECT * FROM order_items;
SELECT * FROM orders;
SELECT * FROM payments;
SELECT * FROM products;