from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.now(),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_lz_to_ods",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl","postgres"],
) as dag:

    # Ensure ODS schemas and constraint tables exist
    init_ods = PostgresOperator(
        task_id="init_ods",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS ods;
        
        -- (re-)create all tables if desired, or use migrations instead
        -- here assumed to exist already
        """,
    )

    # Load products (dedupe on sku)
    upsert_products = PostgresOperator(
        task_id="upsert_products",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO ods.product (product_sku, product_name)
        SELECT DISTINCT product_sku, product_name
        FROM lz.salesorderitem
        WHERE product_sku IS NOT NULL
        ON CONFLICT (product_sku) DO UPDATE 
          SET product_name = EXCLUDED.product_name
          WHERE ods.product.product_name IS DISTINCT FROM EXCLUDED.product_name;
        """,
    )

    # Load customers (clean email + null filtering + dedupe)
    upsert_customers = PostgresOperator(
        task_id="upsert_customers",
        postgres_conn_id="postgres_default",
        sql="""
        WITH cleaned AS (
          SELECT DISTINCT
            id,
            trim(first_name)::VARCHAR(100)   AS first_name,
            trim(last_name)::VARCHAR(100)    AS last_name,
            lower(trim(email))              AS email,
            initcap(trim(gender))           AS gender,
            billing_address,
            shipping_address
          FROM lz.customer
          WHERE email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        )
        INSERT INTO ods.customer (
          id, first_name, last_name, email, gender, billing_address, shipping_address
        )
        SELECT * FROM cleaned
        ON CONFLICT (id) DO UPDATE
          SET first_name      = EXCLUDED.first_name,
              last_name       = EXCLUDED.last_name,
              email           = EXCLUDED.email,
              gender          = EXCLUDED.gender,
              billing_address = EXCLUDED.billing_address,
              shipping_address= EXCLUDED.shipping_address
          WHERE 
            ods.customer.* IS DISTINCT FROM EXCLUDED.*;
        """,
    )

    # Load orders (ensure referential integrity)
    upsert_orders = PostgresOperator(
        task_id="upsert_orders",
        postgres_conn_id="postgres_default",
        sql="""
        WITH cleaned AS (
          SELECT 
            id,
            customer_id,
            order_number,
            created_at,
            modified_at,
            order_total,
            total_qty_ordered
          FROM lz.salesorder
          WHERE order_number IS NOT NULL
            AND modified_at >= created_at
        )
        INSERT INTO ods.salesorder (
          id, customer_id, order_number, created_at, modified_at, order_total, total_qty_ordered
        )
        SELECT c.id, c.customer_id, c.order_number, c.created_at, c.modified_at, c.order_total, c.total_qty_ordered
        FROM cleaned c
        JOIN ods.customer cust ON cust.id = c.customer_id
        ON CONFLICT (id) DO UPDATE
          SET 
            customer_id       = EXCLUDED.customer_id,
            order_number      = EXCLUDED.order_number,
            modified_at       = EXCLUDED.modified_at,
            order_total       = EXCLUDED.order_total,
            total_qty_ordered = EXCLUDED.total_qty_ordered
          WHERE 
            ods.salesorder.* IS DISTINCT FROM EXCLUDED.*;
        """,
    )

    # Load order items (map product and order FKs)
    upsert_order_items = PostgresOperator(
        task_id="upsert_order_items",
        postgres_conn_id="postgres_default",
        sql="""
        WITH raw_items AS (
          SELECT
            item_id,
            order_id,
            product_sku,
            qty_ordered,
            price,
            line_total,
            created_at,
            modified_at
          FROM lz.salesorderitem
        ), mapped AS (
          SELECT
            ri.item_id,
            so.id          AS order_id,
            p.product_id,
            ri.qty_ordered,
            ri.price,
            ri.line_total,
            ri.created_at,
            ri.modified_at
          FROM raw_items ri
          JOIN ods.salesorder so 
            ON so.id = ri.order_id
          JOIN ods.product p 
            ON p.product_sku = ri.product_sku
        )
        INSERT INTO ods.salesorderitem (
          item_id, order_id, product_id, qty_ordered, price, line_total, created_at, modified_at
        )
        SELECT 
          item_id, order_id, product_id, qty_ordered, price, line_total, created_at, modified_at
        FROM mapped
        ON CONFLICT (item_id) DO UPDATE
          SET 
            order_id      = EXCLUDED.order_id,
            product_id    = EXCLUDED.product_id,
            qty_ordered   = EXCLUDED.qty_ordered,
            price         = EXCLUDED.price,
            line_total    = EXCLUDED.line_total,
            modified_at   = EXCLUDED.modified_at
          WHERE 
            ods.salesorderitem.* IS DISTINCT FROM EXCLUDED.*;
        """,
    )

    # Mark last load timestamp (audit/log)
    mark_loaded = PostgresOperator(
        task_id="mark_last_loaded",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS ods.load_audit (
          table_name TEXT PRIMARY KEY,
          last_loaded TIMESTAMP WITH TIME ZONE NOT NULL
        );
        INSERT INTO ods.load_audit (table_name, last_loaded)
          VALUES
            ('product', NOW()),
            ('customer', NOW()),
            ('salesorder', NOW()),
            ('salesorderitem', NOW())
        ON CONFLICT (table_name) DO UPDATE
          SET last_loaded = EXCLUDED.last_loaded;
        """,
    )

    # chain them
    chain(
        init_ods,
        [upsert_products, upsert_customers],
        upsert_orders,
        upsert_order_items,
        mark_loaded,
    )
