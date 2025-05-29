from pyspark.sql.functions import count,col
from airflow.decorators import task
from transform_utils import create_session, load_to_postgres, Extractor, log,Duplicate_check,end_session
import logging
from datetime import datetime

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

        # Rename columns
        suppliers_df=suppliers_df \
                            .withColumnRenamed("supplier_id", "SUPPLIER_ID") \
                            .withColumnRenamed("supplier_name", "SUPPLIER_NAME") \
                            .withColumnRenamed("contact_details", "CONTACT_DETAILS") \
                            .withColumnRenamed("region", "REGION")
            
            
        suppliers_df_tgt=suppliers_df \
                            .select(
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )
            
        # Check for duplicate SUPPLIER_IDs
        checker=Duplicate_check()
        checker.has_duplicates(suppliers_df_tgt, ["SUPPLIER_ID"])    
        
         # Load the cleaned data into the raw.suppliers table
        load_to_postgres(suppliers_df_tgt, "raw.suppliers", "overwrite")
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

         # Rename columns
        products_df=products_df \
                        .withColumnRenamed("product_id", "PRODUCT_ID") \
                        .withColumnRenamed("product_name", "PRODUCT_NAME") \
                        .withColumnRenamed("category", "CATEGORY") \
                        .withColumnRenamed("price", "PRICE") \
                        .withColumnRenamed("stock_quantity", "STOCK_QUANTITY") \
                        .withColumnRenamed("reorder_level", "REORDER_LEVEL") \
                        .withColumnRenamed("supplier_id", "SUPPLIER_ID")
            

        products_df_tgt=products_df \
                                .select(
                                    col("PRODUCT_ID"),
                                    col("PRODUCT_NAME"),
                                    col("CATEGORY"),
                                    col("PRICE"),
                                    col("STOCK_QUANTITY"),
                                    col("REORDER_LEVEL"),
                                    col("SUPPLIER_ID")
                                )

        # Check for duplicate PRODUCT_IDs
        checker=Duplicate_check()
        checker.has_duplicates(products_df_tgt, ["PRODUCT_ID"])
       
         # Load the cleaned data into the raw.products table
        load_to_postgres(products_df_tgt, "raw.products", "overwrite")

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

        customers_df=customers_df \
                        .withColumnRenamed("customer_id", "CUSTOMER_ID") \
                        .withColumnRenamed("name", "NAME") \
                        .withColumnRenamed("city", "CITY") \
                        .withColumnRenamed("email", "EMAIL") \
                        .withColumnRenamed("phone_number", "PHONE_NUMBER") 
            
        customers_df_tgt=customers_df \
                            .select(
                                col("CUSTOMER_ID"),
                                col("NAME"),
                                col("CITY"),
                                col("EMAIL"),
                                col("PHONE_NUMBER")
                            )
        
        # Check for duplicate CUSTOMER_IDs
        checker=Duplicate_check()
        checker.has_duplicates(customers_df_tgt, ["CUSTOMER_ID"])

         # Load the cleaned data into the raw.customers table
        load_to_postgres(customers_df_tgt, "raw.customers", "overwrite")
        return "Task for loading customers got completed successfully."

    except Exception as e:
        logging.error(f"Customers ETL failed: {str(e)}", exc_info=True)
        raise e
    

    finally:
        end_session(spark)

