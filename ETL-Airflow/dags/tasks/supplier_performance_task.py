from airflow.decorators import task
from transform_utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, row_number, current_date, when, lit
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task
def m_load_suppliers_perfomance():
    try:
        spark = create_session()

        # Read raw.sales
        sales_df = read_from_postgres(spark, "raw.sales").select("PRODUCT_ID", "QUANTITY", "ORDER_STATUS")
        log.info("Data Frame: 'sales_df' loaded.")

        # Read raw.products
        products_df = read_from_postgres(spark, "raw.products").select(
            "PRODUCT_ID", "SUPPLIER_ID", "PRODUCT_NAME", "SELLING_PRICE"
        )
        log.info("Data Frame: 'products_df' loaded.")

        # Read raw.suppliers
        suppliers_df = read_from_postgres(spark, "raw.suppliers").select("SUPPLIER_ID", "SUPPLIER_NAME")
        log.info("Data Frame: 'suppliers_df' loaded.")

        # Filter out cancelled orders
        valid_sales_df = sales_df.filter(col("ORDER_STATUS") != "Cancelled")
        log.info("Data Frame: 'valid_sales_df' created.")

        # Join sales and products
        sales_products_df = valid_sales_df.join(products_df, on="PRODUCT_ID", how="left").select(
            "PRODUCT_ID", "SUPPLIER_ID", "PRODUCT_NAME", "QUANTITY", "SELLING_PRICE"
        )
        log.info("Data Frame: 'sales_products_df' created.")

        # Join products-sales with suppliers, calculate REVENUE
        sales_suppliers_df = sales_products_df.join(suppliers_df, on="SUPPLIER_ID", how="right").withColumn(
            "REVENUE", col("QUANTITY") * col("SELLING_PRICE")
        ).select(
            "SUPPLIER_ID", "SUPPLIER_NAME", "PRODUCT_ID", "PRODUCT_NAME", "QUANTITY", "SELLING_PRICE", "REVENUE"
        )
        log.info("Data Frame: 'sales_suppliers_df' created.")

        # Aggregate revenue, quantity, and count of products per supplier
        agg_df = sales_suppliers_df.groupBy("SUPPLIER_ID").agg(
            sum("REVENUE").alias("agg_total_revenue"),
            sum("QUANTITY").alias("agg_total_quantity"),
            countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD")
        )
        log.info("Data Frame: 'agg_df' created.")

        # Top-selling product per supplier based on REVENUE
        revenue_by_product = sales_suppliers_df.groupBy("SUPPLIER_ID", "PRODUCT_NAME").agg(
            sum("REVENUE").alias("PRODUCT_REVENUE")
        )

        product_rank_window = Window.partitionBy("SUPPLIER_ID").orderBy(col("PRODUCT_REVENUE").desc())

        top_products_df = revenue_by_product.withColumn("row_num", row_number().over(product_rank_window)).filter(
            col("row_num") == 1
        ).select(
            "SUPPLIER_ID", col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
        )
        log.info("Data Frame: 'top_products_df' created.")

        # Final target DataFrame
        supplier_perf_df = suppliers_df.join(
            agg_df, on="SUPPLIER_ID", how="left"
        ).join(
            top_products_df, on="SUPPLIER_ID", how="left"
        ).withColumn(
            "DAY_DT", current_date()
        ).select(
            "DAY_DT",
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            col("agg_total_revenue").alias("TOTAL_REVENUE"),
            "TOTAL_PRODUCTS_SOLD",
            col("agg_total_quantity").alias("TOTAL_STOCK_SOLD"),
            when(col("TOP_SELLING_PRODUCT").isNull(), lit("No sales")).otherwise(col("TOP_SELLING_PRODUCT")).alias("TOP_SELLING_PRODUCT")
        ).fillna({
            "TOTAL_REVENUE": 0,
            "TOTAL_PRODUCTS_SOLD": 0,
            "TOTAL_STOCK_SOLD": 0
        })
        log.info("Data Frame: 'supplier_perf_df' created.")

        # Check for duplicates
        checker = Duplicate_check()
        checker.has_duplicates(supplier_perf_df, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(supplier_perf_df, "legacy.supplier_performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        if spark:
            end_session(spark)
