steps:
1. clickhouse: build raw database and its table first
2. ingest raw csv into clickhouse
3. create ods from raw
4. create dwh from ods

note on architecture:
1. raw -> data presented as is, purely from the source (in this case csv)
2. ods (operational data store) -> a processed raw, processed in the sense that it is casted into the correct datatype, has been through quality checks like null rate, valid range of values, etc.
3. dwh (data warehouse) -> in this layer the data starts to resemble business meaning. the tables are joined, aggregated, and restructured, resembling a star schema (facts + dims). facts are tables that store measurable business metrics (such as sales transactions, order qty, revenue, profits, etc), while dims are tables that provide context for the fact (such as customer info, product details, locations, etc).

note on ods layer:
1. 4 tables: orders, items, payments, customers
2. orders is obtained from the raw order, with actual timestamp and computed delivery time
3. payments is obtained from the raw order payments, with the derived column:
    a. is_installment to identify whether the customer paid in installments or all at once.
    b. installment_bucket to group installment counts into categories
    c. is_high_value_payment derived from payment_value and is true on payment value greater within top 25% 
4. The customers ODS table expand the customer records with representative latitude and longitude coordinates derived from the geolocation dataset. Geolocation records are cleaned aggregated by ZIP code prefix, and joined to customers via customer_zip_code_prefix.
5. The ODS order_items table is created from the raw order_items table by standardizing data types, converting shipping_limit_date to a timestamp, and deriving additional metrics related to item cost and shipping. The derived fields include line_total (item price plus freight cost) and freight_ratio (freight cost as a proportion of item price). Records with missing or empty order_id values are removed to ensure data quality.

note on dwh layer:
