steps:
1. clickhouse: build raw database and its table first
2. ingest raw csv into clickhouse
3. create ods from raw

note on ods layer:
1. 4 tables: orders, items, payments, customers
2. orders is obtained from the raw order, with actual timestamp and computed delivery time
3. payments is obtained from the raw order payments, with the derived column:
    a. is_installment to identify whether the customer paid in installments or all at once.
    b. installment_bucket to group installment counts into categories
    c. is_high_value_payment derived from payment_value and is true on payment value greater within top 25% 
4. customers is obtained by combining raw customers table with raw geolocation tables. it has derived field oh latitude and longitude coordinate and zip code for grouping