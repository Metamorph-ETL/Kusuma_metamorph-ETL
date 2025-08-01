from airflow.decorators import task
from tasks.transform_utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, rank, current_date, when, StringType, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from airflow.exceptions import AirflowException

@task(task_id="m_load_suppliers_performance")
def m_load_suppliers_performance():
    try:
        spark = create_session()

        # Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales_pre' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales_pre")\
                                .select(
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("ORDER_STATUS")
                                )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products_pre' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products_pre")\
                                        .select(
                                            col("PRODUCT_ID"),
                                            col("SUPPLIER_ID"),
                                            col("PRODUCT_NAME"),
                                            col("SELLING_PRICE")
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        # Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers_pre' table
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers_pre")\
                                        .select(
                                            col("SUPPLIER_ID"),
                                            col("SUPPLIER_NAME")
                                        )
        log.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built")

        # Processing Node : FIL_Sales_Cancelled -  Filter out records where ORDER_STATUS is 'Cancelled'
        FIL_Sales_Cancelled = SQ_Shortcut_To_Sales\
                                    .filter(
                                        col("ORDER_STATUS") != "Cancelled"
                                    )
        log.info("Data Frame : 'FIL_Cancelled_Sales' is built")

        # Processing Node : JNR_Sales_Products  - Reads data from 'raw.products' and 'raw.sales' tables
        JNR_Sales_Products = FIL_Sales_Cancelled\
                                .join(
                                    SQ_Shortcut_To_Products,
                                    on="PRODUCT_ID",
                                    how="left"
                                )\
                                .select(
                                    FIL_Sales_Cancelled.QUANTITY,
                                    SQ_Shortcut_To_Products.PRODUCT_ID,
                                    SQ_Shortcut_To_Products.SUPPLIER_ID,
                                    SQ_Shortcut_To_Products.PRODUCT_NAME,
                                    SQ_Shortcut_To_Products.SELLING_PRICE
                                )
        log.info("Data Frame : 'JNR_Sales_Products' is built")

        # Processing Node : JNR_Products_Suppliers  - Reads data from JNR_Sales_Products and 'raw.suppliers' tables
        JNR_Products_Suppliers = JNR_Sales_Products\
                                    .join(
                                        SQ_Shortcut_To_Suppliers,
                                        on="SUPPLIER_ID",
                                        how="right"
                                    )\
                                    .withColumn(
                                        "REVENUE", col("QUANTITY") * col("SELLING_PRICE")
                                    )\
                                    .select(
                                        JNR_Sales_Products.PRODUCT_ID,
                                        JNR_Sales_Products.PRODUCT_NAME,
                                        JNR_Sales_Products.QUANTITY,
                                        JNR_Sales_Products.SELLING_PRICE,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_ID,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_NAME,
                                        (col("QUANTITY") * col("SELLING_PRICE")).alias("REVENUE")
                                    )
        log.info("Data Frame : 'JNR_Products_Suppliers' is built")

        # Processing Node : AGG_TRANS_Supplier_Product - Aggregates data at the product level per supplier
        AGG_TRANS_Supplier_Product = JNR_Products_Suppliers\
                                        .groupBy("SUPPLIER_ID")\
                                        .agg(
                                            sum("REVENUE").alias("agg_TOTAL_REVENUE"),
                                            sum("QUANTITY").alias("agg_TOTAL_STOCK_SOLD"),
                                            countDistinct("PRODUCT_ID").alias("agg_TOTAL_PRODUCTS_SOLD")
                                        )
        log.info("Data Frame : 'AGG_TRANS_Product' is built")     

        window_spec = Window.partitionBy("SUPPLIER_ID").orderBy(col("REVENUE").desc(), col("PRODUCT_NAME"))

        #Processing Node : Top_Product - Filters to get the top selling product per supplier
        Top_Product = JNR_Products_Suppliers\
                            .withColumn("rank", row_number().over(window_spec))\
                            .filter(col("rank") == 1)\
                            .select(
                                col("SUPPLIER_ID"),
                                col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
                            )
        log.info("Data Frame : 'Top_Product' is built")

        # Processing Node : Supplier_with_Aggregates - Adds aggregate sales info per supplier
        Supplier_with_Aggregates = SQ_Shortcut_To_Suppliers\
                                    .join(
                                        AGG_TRANS_Supplier_Product,
                                        on="SUPPLIER_ID",
                                        how="left"
                                    )\
                                    .select(
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_ID,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_NAME,
                                        AGG_TRANS_Supplier_Product["agg_TOTAL_REVENUE"],
                                        AGG_TRANS_Supplier_Product["agg_TOTAL_PRODUCTS_SOLD"],
                                        AGG_TRANS_Supplier_Product["agg_TOTAL_STOCK_SOLD"]
                                    )
                                    
        # Processing Node : Supplier_with_Top_Product - Adds top-selling product info to suppliers
        Supplier_with_Top_Product = Supplier_with_Aggregates\
                                        .join(
                                            Top_Product,
                                            on="SUPPLIER_ID",
                                            how="left"
                                        )\
                                        .select(
                                            Supplier_with_Aggregates.SUPPLIER_ID,
                                            Supplier_with_Aggregates.SUPPLIER_NAME,
                                            Supplier_with_Aggregates["agg_TOTAL_REVENUE"],
                                            Supplier_with_Aggregates["agg_TOTAL_PRODUCTS_SOLD"],
                                            Supplier_with_Aggregates["agg_TOTAL_STOCK_SOLD"],
                                            Top_Product.TOP_SELLING_PRODUCT
                                        )
                                        
        # Processing Node : Supplier_Performance - Adds DAY_DT, fills missing values, replaces nulls in TOP_SELLING_PRODUCT
        Supplier_Performance = Supplier_with_Top_Product\
                                        .withColumn(
                                            "TOP_SELLING_PRODUCT",
                                            when(col("TOP_SELLING_PRODUCT").isNull(), lit("No sales"))
                                            .otherwise(col("TOP_SELLING_PRODUCT"))
                                            .cast(StringType())
                                        )\
                                        .withColumn(
                                            "DAY_DT", current_date()
                                        )\
                                        .fillna({
                                            "agg_TOTAL_REVENUE": 0,
                                            "agg_TOTAL_PRODUCTS_SOLD": 0,
                                            "agg_TOTAL_STOCK_SOLD": 0
                                        })

        # Processing Node : Supplier_Performance - Rename back to match target database schema
        Supplier_Performance = Supplier_Performance\
                                    .withColumnRenamed("agg_TOTAL_REVENUE", "TOTAL_REVENUE")\
                                    .withColumnRenamed("agg_TOTAL_PRODUCTS_SOLD", "TOTAL_PRODUCTS_SOLD")\
                                    .withColumnRenamed("agg_TOTAL_STOCK_SOLD", "TOTAL_STOCK_SOLD")

        # Processing Node : 'Shortcut_To_Supplier_Performance_Tgt' - Filtered dataframe for target table
        Shortcut_To_Supplier_Performance_Tgt = Supplier_Performance\
                                                .select(
                                                    col("DAY_DT"),
                                                    col("SUPPLIER_ID"),
                                                    col("SUPPLIER_NAME"),
                                                    col("TOTAL_REVENUE"),
                                                    col("TOTAL_PRODUCTS_SOLD"),
                                                    col("TOTAL_STOCK_SOLD")
                                                )
        log.info("Data Frame : 'Shortcut_To_Supplier_Performance_Tgt' is built")

        # Check for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.supplier_performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance task failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
        end_session(spark)
