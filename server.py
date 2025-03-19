import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from pathlib import Path

# Create a FastAPI instance
app = FastAPI()

# Define the directory path containing CSV files
CSV_PATH_FILE = Path("D:/ETL_pipeline_project/CSV_FILE_PATH/")

# Define the Product model
class Product(BaseModel):
    product_ID: int
    product_name: str
    category: str
    price: float
    stock_quantity: int
    reorder_level: int
    supplier_id: int

# Define the Customer model
class Customer(BaseModel):
    customer_ID: int
    name: str
    city: str
    email: EmailStr
    phone_number: str
    loyality_tier:str

# Define the Supplier model
class Supplier(BaseModel):
    supplier_ID: int
    supplier_name: str
    contact_details: str
    region: str

@app.get("/products")
def get_products_from_csv():
    # Find all CSV files matching the pattern "products*.csv" 
    csv_files = list(CSV_PATH_FILE.glob("products*.csv")) 
    if not csv_files:
        raise HTTPException(status_code=404, detail="No matching CSV files found")
    
    # Read the latest matching CSV file
    latest_csv = max(csv_files, key=lambda f: Path(f).stat().st_mtime)  # Get the most recently modified file
    df = pd.read_csv(latest_csv)

    if df.empty:
        raise HTTPException(status_code=404, detail="No products found in the CSV file")

    # Convert DataFrame to a list of dictionaries
    products_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "products": products_list
    }

@app.get("/customers")
def get_customers_from_csv():
    # Find all CSV files matching "customers*.csv"
    csv_files = list(CSV_PATH_FILE.glob("customers*.csv"))
    
    if not csv_files:
        raise HTTPException(status_code=404, detail="No matching CSV files found")
    
    # Read the latest matching CSV file
    latest_csv =max(csv_files, key=lambda f:  Path(f).stat().st_mtime)  # Get the most recently modified file
    df = pd.read_csv(latest_csv)

    # Drop "Loyalty_Tier" column if it exists
    if "loyalty_tier" in df.columns:
        df.drop(columns=["loyalty_tier"], inplace=True)

    # Ensure the required columns exist
    required_columns = {"customer_id", "name", "city", "email", "phone_number"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=404, detail="CSV file is missing required columns")
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise HTTPException(status_code=400, detail=f"CSV file is missing required columns: {missing_columns}")

    if df.empty:
        raise HTTPException(status_code=404, detail="No customers found in the CSV file")

    # Convert DataFrame to a list of dictionaries
    customers_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "customers": customers_list
    }

@app.get("/suppliers")
def get_suppliers_from_csv():
    # Find all CSV files matching "suppliers*.csv"
    csv_files = list(CSV_PATH_FILE.glob("suppliers*.csv"))
    if not csv_files:
        raise HTTPException(status_code=404, detail="No matching CSV files found")
    
    # Read the latest matching CSV file
    latest_csv=max(csv_files, key=lambda f: Path(f).stat().st_mtime)
    df = pd.read_csv(latest_csv)

    if df.empty:
        raise HTTPException(status_code=404, detail="No suppliers found in the CSV file")

    # Convert DataFrame to a list of dictionaries
    suppliers_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "suppliers": suppliers_list
    }