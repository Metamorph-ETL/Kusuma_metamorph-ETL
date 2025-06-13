from airflow.decorators import task
from transform_utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, countDistinct, row_number, current_date, when, lit
from pyspark.sql.window import Window
from airflow.exceptions import AirflowException

@task (task_id="m_load_suppliers_perfomance")
def m_load_suppliers_perfomance():
    try:
        spark = create_session()

        # Processing Node : SQ_Shortcut_To_Sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales")\
                                    .select(
                                        col("PRODUCT_ID"),
                                        col("QUANTITY"),
                                        col("ORDER_STATUS")
                                    )
        log.info("Data Frame : 'SQ_Shortcut_To_Sales' is built")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")\
                                      .select(
                                          col("PRODUCT_ID"),
                                          col("SUPPLIER_ID"),
                                          col("PRODUCT_NAME"),
                                          col("SELLING_PRICE")
                                      )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")

        # Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers' table
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers")\
                                       .select(
                                           col("SUPPLIER_ID"),
                                           col("SUPPLIER_NAME")
                                       )
        log.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built")

        # Processing Node : FIL_Sales_Cancelled - Filters out records where ORDER_STATUS is 'Cancelled'
        FIL_Sales_Cancelled = SQ_Shortcut_To_Sales\
                                  .filter(
                                      col("ORDER_STATUS") != "Cancelled"
                                  )
        log.info("Data Frame : 'FIL_Sales_Cancelled' is built")

        # Processing Node : JNR_Sales_Products - Joins filtered sales with products
        JNR_Sales_Products = FIL_Sales_Cancelled\
                                .join(
                                    SQ_Shortcut_To_Products,
                                    on="PRODUCT_ID",
                                    how="left"
                                )\
                                .select(
                                    col("PRODUCT_ID"),
                                    col("SUPPLIER_ID"),
                                    col("PRODUCT_NAME"),
                                    col("QUANTITY"),
                                    col("SELLING_PRICE")
                                )
        log.info("Data Frame : 'JNR_Sales_Products' is built")

        # Processing Node : JNR_Products_Suppliers - Joins products-sales with suppliers and calculates revenue
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
                                         col("SUPPLIER_ID"),
                                         col("SUPPLIER_NAME"),
                                         col("PRODUCT_ID"),
                                         col("PRODUCT_NAME"),
                                         col("QUANTITY"),
                                         col("SELLING_PRICE"),
                                         col("REVENUE")
                                     )
        log.info("Data Frame : 'JNR_Products_Suppliers' is built")

        # Processing Node : AGG_TRANS_Product - Aggregates revenue, quantity, and distinct products
        AGG_TRANS_Product = JNR_Products_Suppliers\
                                .groupBy("SUPPLIER_ID")\
                                .agg(
                                    sum("REVENUE").alias("agg_total_revenue"),
                                    sum("QUANTITY").alias("agg_total_quantity"),
                                    countDistinct("PRODUCT_ID").alias("TOTAL_PRODUCTS_SOLD")
                                )
        log.info("Data Frame : 'AGG_TRANS_Product' is built")

        # Processing Node : AGG_TRANS_Product - Calculates total revenue per product per supplier
        AGG_TRANS_Product = JNR_Products_Suppliers\
                                 .groupBy("SUPPLIER_ID", "PRODUCT_NAME")\
                                 .agg(
                                     sum("REVENUE").alias("PRODUCT_REVENUE")
                                 )

        # Processing Node : RNK_Suppliers - Ranks products by revenue within each supplier
        RNK_Suppliers = Window.partitionBy("SUPPLIER_ID")\
                              .orderBy(col("PRODUCT_REVENUE").desc())

        # Processing Node : Top_Selling_Product_df - Filters top selling product per supplier
        Top_Selling_Product_df = AGG_TRANS_Product\
                                     .withColumn("row_num", row_number().over(RNK_Suppliers))\
                                     .filter(col("row_num") == 1)\
                                     .select(
                                         col("SUPPLIER_ID"),
                                         col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
                                     )
        log.info("Data Frame : 'Top_Selling_Product_df' is built")

        # Processing Node : Shortcut_To_Supplier_Performance_Tgt - Final target DataFrame
        Shortcut_To_Supplier_Performance_Tgt = SQ_Shortcut_To_Suppliers\
                                                   .join(
                                                       AGG_TRANS_Product,
                                                       on="SUPPLIER_ID",
                                                       how="left"
                                                   )\
                                                   .join(
                                                       Top_Selling_Product_df,
                                                       on="SUPPLIER_ID",
                                                       how="left"
                                                   )\
                                                   .withColumn("DAY_DT", current_date())\
                                                   .select(
                                                       col("DAY_DT"),
                                                       col("SUPPLIER_ID"),
                                                       col("SUPPLIER_NAME"),
                                                       col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                       col("TOTAL_PRODUCTS_SOLD"),
                                                       col("agg_total_quantity").alias("TOTAL_STOCK_SOLD"),
                                                       when(col("TOP_SELLING_PRODUCT").isNull(), lit("No sales"))
                                                       .otherwise(col("TOP_SELLING_PRODUCT"))
                                                       .alias("TOP_SELLING_PRODUCT")
                                                   )\
                                                   .fillna({
                                                       "TOTAL_REVENUE": 0,
                                                       "TOTAL_PRODUCTS_SOLD": 0,
                                                       "TOTAL_STOCK_SOLD": 0
                                                   })
        log.info("Data Frame : 'Shortcut_To_Supplier_Performance_Tgt' is built")

        # Processing Node : DUPLICATE_CHECK - Checks for duplicates before load
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_Supplier_Performance_Tgt, ["SUPPLIER_ID", "DAY_DT"])

        # Processing Node : TARGET_LOAD - Loads final data to PostgreSQL
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.supplier_performance", "append")

        return "Supplier performance task completed successfully."

    except Exception as e:
        log.error(f"Supplier performance calculation failed: {str(e)}", exc_info=True)
        raise AirflowException("Supplier_performance ETL failed")

    finally:
            end_session(spark)
