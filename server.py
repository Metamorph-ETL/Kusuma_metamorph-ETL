import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os


# Create a FastAPI instance
app = FastAPI()

# CSV File Path 
CSV_FILE_PATH = "C:/ETL pipeline project/CSV_FILE_PATH"  

# Define the Product model
class products(BaseModel):
    Product_ID: int
    Product_Name: str
    Category: str
    Price: float
    Stock_Quantity: int
    Reorder_Level: int
    Supplier_ID: int

@app.get("/products")
def get_products_from_csv():
    # Checking if file exists
    if not os.path.exists(CSV_FILE_PATH):
        raise HTTPException(status_code=400, detail="CSV file not found")
    
    # Read CSV into a DataFrame
    df = pd.read_csv(CSV_FILE_PATH)

    # Check if the DataFrame is empty
    if df.empty:
        raise HTTPException(status_code=400, detail="No products found in the CSV file")

    # Convert DataFrame to a list of dictionaries
    products_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "products": products_list  # Return the actual data
    }
