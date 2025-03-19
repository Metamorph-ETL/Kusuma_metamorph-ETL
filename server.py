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

