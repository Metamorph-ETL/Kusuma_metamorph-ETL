from pyspark.sql.functions import col, current_date
from airflow.decorators import task
from tasks.transform_utils import create_session, load_to_postgres, Extractor, log,Duplicate_check,end_session
import logging
from datetime import datetime

# Normalize column names: trim, uppercase, replace spaces with underscores
def normalize_column_names(df):
    for col_name in df.columns:
        normalized = col_name.strip().upper().replace(" ", "_")
        df = df.withColumnRenamed(col_name, normalized)
    return df

#create a task that ingests data into raw.suppliers table
@task
def m_ingest_data_into_suppliers():
    try:
        spark = create_session()

        # Extract supplier data from API
        extractor = Extractor("/v1/suppliers")

        data = extractor.extract_data()
        
         # Convert extracted JSON data to Spark DataFrame
        suppliers_df = spark.createDataFrame(data)

    # Normalize column names: trim, uppercase, and replace spaces with underscores
        suppliers_df = normalize_column_names(suppliers_df)
            
        suppliers_df_tgt = suppliers_df \
                            .select(
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )

        # Adding a column "DAY_DT" with the current date to track daily snapshots
        suppliers_legacy_df = suppliers_df_tgt \
                               .withColumn("DAY_DT", current_date())

         # Rearranging and selecting final columns for writing to the legacy table
        suppliers_legacy_df_tgt = suppliers_legacy_df \
                                    .select(
                                        col("DAY_DT"),
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME"),
                                        col("CONTACT_DETAILS"),
                                        col("REGION")
                                    )
            
        # Check for duplicate SUPPLIER_IDs
        checker = Duplicate_check()
        checker.has_duplicates(suppliers_df_tgt, ["SUPPLIER_ID"])    
        
         # Load the cleaned data into the raw.suppliers table
        load_to_postgres(suppliers_df_tgt, "raw.suppliers_pre", "overwrite")
     
         # Load the cleaned data into the legacy.suppliers table
        load_to_postgres(suppliers_legacy_df_tgt, "legacy.suppliers", "append")

        return "Task for loading Suppliers got completed successfully."


    except Exception as e:
        logging.error(f"Suppliers ETL failed: {str(e)}", exc_info=True)
        raise e 

    finally:
        end_session(spark)

#create a task that ingests data into raw.products table
@task
def m_ingest_data_into_products():
    try:
        spark = create_session()
        
        # Extract products data from API
        extractor = Extractor("/v1/products")

        data = extractor.extract_data()
         
        # Convert extracted JSON data to Spark DataFrame
        products_df = spark.createDataFrame(data)

        # Normalize column names: trim, uppercase, and replace spaces with underscores
        products_df = normalize_column_names(products_df)

        products_df_tgt = products_df \
                                .select(
                                    col("PRODUCT_ID"),
                                    col("PRODUCT_NAME"),
                                    col("CATEGORY"),
                                    col("SELLING_PRICE"),
                                    col("COST_PRICE"),
                                    col("STOCK_QUANTITY"),
                                    col("REORDER_LEVEL"),
                                    col("SUPPLIER_ID")
                                )
        
        # Normalize column names: trim, uppercase, and replace spaces with underscores
        products_legacy_df = products_df_tgt \
                               .withColumn("DAY_DT", current_date())

        # Rearranging and selecting the final columns for writing to the legacy table
        products_legacy_df_tgt = products_legacy_df \
                                    .select(
                                        col("DAY_DT"),
                                        col("PRODUCT_ID"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("SELLING_PRICE"),
                                        col("COST_PRICE"),
                                        col("STOCK_QUANTITY"),
                                        col("REORDER_LEVEL"),
                                        col("SUPPLIER_ID")
                                    )

        # Check for duplicate PRODUCT_IDs
        checker = Duplicate_check()
        checker.has_duplicates(products_df_tgt, ["PRODUCT_ID"])
       
         # Load the cleaned data into the raw.products table
        load_to_postgres(products_df_tgt, "raw.products_pre", "overwrite")

         # Load the cleaned data into the legacy.products table
        load_to_postgres(products_legacy_df_tgt, "legacy.products", "append")

        return "Task for loading products got completed successfully."

    except Exception as e:
        logging.error(f"Products ETL failed: {str(e)}", exc_info=True)
        raise e
    

    finally:
        end_session(spark)

#create a task that ingests data into raw.customers table
@task
def m_ingest_data_into_customers():
    try:
        spark = create_session()
        extractor = Extractor("/v1/customers")
        data = extractor.extract_data()
        customers_df = spark.createDataFrame(data)

        # Normalize column names: trim, uppercase, and replace spaces with underscores
        customers_df = normalize_column_names(customers_df)

        customers_df_tgt=customers_df \
                            .select(
                                col("CUSTOMER_ID"),
                                col("NAME"),
                                col("CITY"),
                                col("EMAIL"),
                                col("PHONE_NUMBER")
                            )
        
         # Adding a column "DAY_DT" with the current date to track daily snapshots                   
        customers_legacy_df = customers_df_tgt \
                               .withColumn("DAY_DT", current_date())
        
        # Rearranging and selecting the final columns for writing to the legacy table          
        customers_legacy_df_tgt = customers_legacy_df \
                                    .select(
                                        col("DAY_DT"),
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY"),
                                        col("EMAIL"),
                                        col("PHONE_NUMBER")
                                    )
        
        # Check for duplicate CUSTOMER_IDs
        checker=Duplicate_check()
        checker.has_duplicates(customers_legacy_df_tgt, ["CUSTOMER_ID"])

         # Load the cleaned data into the raw.customers table
        load_to_postgres(customers_df_tgt, "raw.customers_pre", "overwrite")

         # Load the cleaned data into the legacy.customers table
        load_to_postgres(customers_legacy_df_tgt, "legacy.customers", "append")

        return "Task for loading customers got completed successfully."

    except Exception as e:
        logging.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise e
    

    finally:
        end_session(spark)

#create a task that ingests data into raw.sales table
@task
def m_ingest_data_into_sales():
    try:
        spark = create_session()

       # Define the GCS bucket name
        GCS_BUCKET_NAME = "meta-morph-flow"
        today_str = datetime.today().strftime("%Y%m%d")

        #GCS path to the sales CSV file for today's date
        gcs_path = f"gs://meta-morph-flow/{today_str}/sales_{today_str}.csv"

        log.info(f"Reading sales CSV from path: {gcs_path}")

        # Load sales data from GCS
        sales_df =spark.read.csv(gcs_path, header=True, inferSchema=True)

        # Rename columns to match schema standards (uppercase), and select the required columns
        sales_df = normalize_column_names(sales_df)
            
        sales_df_tgt = sales_df \
                        .select(
                            col("SALE_ID"),
                            col("CUSTOMER_ID"),
                            col("PRODUCT_ID"),
                            col("SALE_DATE"),
                            col("QUANTITY"),
                            col("DISCOUNT"),
                            col("SHIPPING_COST"),
                            col("ORDER_STATUS"),
                            col("PAYMENT_MODE")
                        )
      # Adding a column "DAY_DT" with the current date to track daily snapshots
        sales_legacy_df = sales_df_tgt \
                               .withColumn("DAY_DT", current_date())

        # Rearranging and selecting final columns for writing to the legacy table
        sales_legacy_df_tgt = sales_legacy_df \
                                 .select(
                                     col("DAY_DT"),
                                     col("SALE_ID"),
                                     col("CUSTOMER_ID"),
                                     col("PRODUCT_ID"),
                                     col("SALE_DATE"),
                                     col("QUANTITY"),
                                     col("DISCOUNT"),
                                     col("SHIPPING_COST"),
                                     col("ORDER_STATUS"),
                                     col("PAYMENT_MODE")
                                 )

        # Check for duplicate SALES_IDs
        checker = Duplicate_check()
        checker.has_duplicates(sales_df_tgt, ["SALE_ID"])

        # Load to PostgreSQL: raw.sales
        load_to_postgres(sales_legacy_df_tgt, "raw.sales_pre", "overwrite")

         # Load the cleaned data into the legacy.sales table
        load_to_postgres(sales_legacy_df_tgt, "legacy.sales", "append")


        return "Task for loading sales got completed successfully."

    except Exception as e:
        logging.error(f"Sales ETL failed: {str(e)}", exc_info=True)
        raise e

    finally:
        end_session(spark)