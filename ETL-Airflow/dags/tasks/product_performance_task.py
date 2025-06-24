from airflow.decorators import task
from transform_utils import create_session, load_to_postgres, Duplicate_check, end_session, log, read_from_postgres
from pyspark.sql.functions import sum, col, current_date, when, lit, avg
from airflow.exceptions import AirflowException

@task(task_id="m_load_product_performance")
def m_load_product_performance():
    try:
        spark = create_session()

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales")\
                                .select(
                                        col("PRODUCT_ID"),
                                        col("QUANTITY")
                                )
        log.info("Data Frame : 'SQ_Shortcut_To_sales' is built")

       # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")\
                                    .select(
                                            col("PRODUCT_ID"), 
                                            col("PRODUCT_NAME"), 
                                            col("SELLING_PRICE"), 
                                            col("COST_PRICE"), 
                                            col("STOCK_QUANTITY"), 
                                            col("REORDER_LEVEL"), 
                                            col("CATEGORY")
                                    )
        log.info("Data Frame : 'SQ_Shortcut_To_Products' is built")


         # Processing Node : FIL_Cancelled_Sales - Filters out cancelled orders
        FIL_Cancelled_Sales = SQ_Shortcut_To_sales\
                                            .filter(
                                                col("ORDER_STATUS") != "Cancelled"
                                            )
                                
        log.info("Data Frame : 'FIL_Cancelled_Sales' is built")

        # Processing Node : JNR_Sales_Products - Joins sales data with product data on PRODUCT_ID
        JNR_Sales_Products =  SQ_Shortcut_To_Products\
                                .join( 
                                    FIL_Cancelled_Sales, 
                                    on="PRODUCT_ID",
                                    how="left"
                                )\
                                .select(
                                    SQ_Shortcut_To_Products.PRODUCT_ID,
                                    SQ_Shortcut_To_Products.PRODUCT_NAME,  
                                    SQ_Shortcut_To_Products.COST_PRICE,                                   
                                    SQ_Shortcut_To_Products.SELLING_PRICE,
                                    SQ_Shortcut_To_Products.CATEGORY,
                                    SQ_Shortcut_To_Products.STOCK_QUANTITY, 
                                    SQ_Shortcut_To_Products.REORDER_LEVEL,
                                    FIL_Cancelled_Sales.QUANTITY
                                )\
                                .withColumn("TOTAL_SALES_AMOUNT", col("QUANTITY") * col("SELLING_PRICE"))\
                                .withColumn("PROFIT", col("QUANTITY") * (col("SELLING_PRICE") - col("COST_PRICE")))        
        log.info(f"Data Frame : 'JNR_Sales_Products' is built")

        # Processing Node :  AGG_Product_Performance - Aggregate sales metrics by product
        AGG_product_performance = JNR_Sales_Products\
                                        .groupBy(
                                            col("PRODUCT_ID"), 
                                            col("PRODUCT_NAME"), 
                                            col("CATEGORY"),
                                            col("STOCK_QUANTITY"),
                                            col("REORDER_LEVEL")
                                        )\
                                        .agg(
                                            sum("QUANTITY").alias("agg_QUANTITY_SOLD"),
                                            sum(col("QUANTITY") * col("SELLING_PRICE")).alias("SALES_AMOUNT"),
                                            sum("TOTAL_SALES_AMOUNT").alias("agg_TOTAL_SALES_AMOUNT"),
                                            avg("SELLING_PRICE").alias("agg_SELLING_PRICE"),
                                            sum("PROFIT").alias("agg_PROFIT")
                                        )
        log.info("Data Frame : 'AGG_product_performance' is built")

        # Processing Node : JNR_Product_Agg_Performance - Add stock level status and load current date
        JNR_Product_Agg_Performance = AGG_product_performance\
                                        .withColumn(
                                        "Stock_Level_Status",
                                        when(col("STOCK_QUANTITY") <= col("REORDER_LEVEL"), lit("Below Reorder Level"))
                                        .otherwise(lit("Sufficient Stock"))
                                        )\
                                        .withColumn("DAY_DT", current_date())   
        log.info("Data Frame : 'JNR_Product_Agg_Performance' is built")

        # Processing Node : Product_Performance - Rename columns to match target database schema
        product_performance = JNR_Product_Agg_Performance\
                                    .withColumnRenamed("agg_TOTAL_SALES_AMOUNT", "TOTAL_SALES_AMOUNT")\
                                    .withColumnRenamed("agg_QUANTITY_SOLD", "TOTAL_QUANTITY_SOLD")\
                                    .withColumnRenamed("agg_SELLING_PRICE", "AVG_SALE_PRICE")\
                                    .withColumnRenamed("agg_PROFIT", "PROFIT")\
                                    .withColumnRenamed("Stock_Level_Status", "STOCK_LEVEL_STATUS")

        # Processing Node : Shortcut_To_Supplier_Performance_Tgt - Selects and arranges final columns as per target table schema
        Shortcut_To_product_performance_Tgt = product_performance\
                                                    .select(
                                                        col("PRODUCT_ID"),
                                                        col("PRODUCT_NAME"),
                                                        col("TOTAL_SALES_AMOUNT"),
                                                        col("TOTAL_QUANTITY_SOLD"),
                                                        col("AVG_SALE_PRICE"),
                                                        col("STOCK_QUANTITY"),
                                                        col("REORDER_LEVEL"),
                                                        col("STOCK_LEVEL_STATUS"),
                                                        col("PROFIT"),
                                                        col("CATEGORY"),
                                                        col("DAY_DT")
                                                    )
        log.info("Data Frame : 'Shortcut_To_product_performance_Tgt' is built")

        # Check for duplicates
        checker = Duplicate_check()
        checker.has_duplicates(Shortcut_To_product_performance_Tgt, ["PRODUCT_ID", "DAY_DT"])

        # Load to PostgreSQL
        load_to_postgres(Shortcut_To_product_performance_Tgt, "legacy.product_performance", "append")

    except Exception as e:
        log.error(f"Product performance task failed: {str(e)}", exc_info=True)
        raise AirflowException("Product_performance ETL failed")

    finally:
        end_session(spark)
