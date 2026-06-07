from .ch_client import get_client

def check_row_counts(client, thresholds: dict):
    for table, min_rows in thresholds.items():
        result = client.query(f"SELECT count() FROM {table}").result_rows[0][0]
        if result < min_rows:
            print(f"[QUALITY CHECK WARNING] {table} has {result} rows, expected >= {min_rows}")

def check_nulls(client, checks: list[tuple]):
    for table, column, max_pct in checks:
        result = client.query(f"""
            SELECT countIf({column} IS NULL) / count() AS null_pct
            FROM {table}
        """).result_rows[0][0]
        if result > max_pct:
            raise ValueError(f"[QUALITY CHECK FAIL] {table}.{column} null rate is {result:.2%}, max allowed is {max_pct:.2%}")

def check_duplicates(client, checks: list[tuple]):
    for table, column in checks:
        result = client.query(f"""
            SELECT count() - count(DISTINCT {column})
            FROM {table}
        """).result_rows[0][0]
        if result > 0:
            raise ValueError(f"[QUALITY CHECK FAIL] {table}.{column} has {result} duplicate values")

def check_valid_values(client, checks: list[tuple]):
    for table, column, allowed in checks:
        allowed_str = ", ".join(f"'{v}'" for v in allowed)
        result = client.query(f"""
            SELECT count()
            FROM {table}
            WHERE {column} IS NOT NULL
            AND {column} NOT IN ({allowed_str})
        """).result_rows[0][0]
        if result > 0:
            raise ValueError(
                f"[DQ FAIL] {table}.{column} has {result} rows with unexpected values"
            )

def check_referential_integrity(client, checks: list[tuple]):
    for child_table, child_col, parent_table, parent_col in checks:
        result = client.query(f"""
            SELECT count()
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
            WHERE p.{parent_col} IS NULL
        """).result_rows[0][0]
        if result > 0:
            raise ValueError(
                f"[DQ FAIL] {child_table}.{child_col} has {result} orphan rows "
                f"not found in {parent_table}.{parent_col}"
            )
        
def run_all_checks(database: str = "default"):
    client = get_client(database)

    check_row_counts(client, {
        "raw.orders": 99441,
        "raw.order_items": 112650,
        "raw.order_payments": 103886,
        "raw.order_reviews": 99224,
        "raw.customers": 99441,
        "raw.sellers": 3095,
        "raw.products": 32951,
        "raw.geolocation": 1000163,
        "raw.product_category_translation": 71,
    })

    check_nulls(client, [
        ("raw.orders", "order_id", 0.0),
        ("raw.orders", "customer_id", 0.0),
        ("raw.order_payments", "order_id", 0.0),
        ("raw.order_payments", "payment_value", 0.0),
        ("raw.order_items", "order_id", 0.0),
        ("raw.order_items", "price", 0.0),
        ("raw.customers", "customer_id", 0.0),
        ("raw.sellers", "seller_id", 0.0),
        ("raw.products", "product_id", 0.0),
    ])

    check_duplicates(client, [
        ("raw.orders", "order_id"),
    ])

    check_valid_values(client, [
        ("raw.order_reviews", "review_score", ["1", "2", "3", "4", "5"]),
        ("raw.order_payments", "payment_type", 
            ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]),
        ("raw.orders", "order_status",
            ["delivered", "shipped", "canceled", "unavailable", 
             "invoiced", "processing", "created", "approved"]),
    ])


    check_referential_integrity(client, [
        ("raw.order_items", "order_id", "raw.orders", "order_id"),
        ("raw.order_payments", "order_id", "raw.orders", "order_id"),
    ])

    print("[QUALITY CHECK] All checks passed.")