<<<<<<< HEAD
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Create app
app = FastAPI()

# Secret key and algo
SECRET_KEY = "Project-Metamorph"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# OAuth2PasswordBearer instance
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Define the directory path containing CSV files
CSV_PATH_FILE = Path("D:/ETL_pipeline_project/CSV_FILE_PATH")
# user
user = {
    "username": "admin",
    "password": "admin"
}

# Function to create JWT token
def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Login endpoint (/token)
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username != user["username"] or form_data.password != user["password"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username},
        expires_delta=access_token_expires
=======
import io
from datetime import datetime, timedelta
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from dotenv import load_dotenv
from os import environ as env


from utils import (
    get_current_active_user,
    create_access_token,
    authenticate_user,
    User,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Authentication models and utils
class Token(BaseModel):
    access_token: str
    token_type: str


# GCS Configuration
GCS_BUCKET_NAME = "meta-morph"
GCS_CREDENTIALS_PATH = env["GCS_CREDENTIALS_PATH"]

def get_gcs_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCS_CREDENTIALS_PATH
        )
        return storage.Client(credentials=credentials)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS client error: {str(e)}")

def get_latest_file_from_gcs(file_keyword: str):
    try:
    
        client = get_gcs_client()
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        today_str = "20250322"
        blob_path = f"{today_str}/{file_keyword}_{today_str}.csv"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise HTTPException(status_code=404,detail=f"File {blob_path} not found in GCS.")
        return blob
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GCS access error: {str(e)}")

def read_csv_from_gcs(blob):
    try:
        content = blob.download_as_string()
        return pd.read_csv(io.StringIO(content.decode('utf-8')))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV read error: {str(e)}")

# Login endpoint (/token)
@app.post("/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user({"admin": {"username": "admin", "password": "admin"}}, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username},
        expires_delta=access_token_expires,
>>>>>>> c6acce87d3fc6e39a88cccaa4e37b7a71d923f4e
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


<<<<<<< HEAD
@app.get("/v1/customers")
async def read_customers(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
 # Find all CSV files matching "customers*.csv" in the specified folder
    csv_files = list(CSV_PATH_FILE.glob("customers*.csv"))
    
    if not csv_files:
        raise HTTPException(status_code=404, detail="No matching CSV files found")
    
    # Read the most recently modified CSV file
    latest_csv = max(csv_files, key=lambda f: f.stat().st_mtime)  # Get the most recently modified file
    df = pd.read_csv(latest_csv)
    # Drop the "Loyalty_Tier" column if it exists
    if "loyalty_tier" in df.columns:
        df.drop(columns=["loyalty_tier"], inplace=True)

    # Ensure the CSV contains the required columns
    required_columns = {"customer_id", "name", "city", "email", "phone_number"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=404, detail="CSV file is missing required columns")
    
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise HTTPException(status_code=404, detail=f"CSV file is missing required columns: {missing_columns}")

    if df.empty:
        raise HTTPException(status_code=404, detail="No customers found in the CSV file")

    # Convert DataFrame to a list of dictionaries (one for each row)
    customers_list = df.to_dict(orient="records")

    return {
        "status": 200,
        "customers": customers_list
    }
=======
@app.get("/v1/products")
def get_products():
    blob = get_latest_file_from_gcs("product")
    df = read_csv_from_gcs(blob)
    return {"status": 200, "data": df.to_dict(orient="records")}

@app.get("/v1/customers")
def get_customers(current_user: User = Depends(get_current_active_user)):
    blob = get_latest_file_from_gcs("customer")
    df = read_csv_from_gcs(blob)
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)
    return {"status": 200, "data": df.to_dict(orient="records")}

@app.get("/v1/suppliers")
def get_suppliers():
    blob = get_latest_file_from_gcs("supplier")
    df = read_csv_from_gcs(blob)
    return {"status": 200, "data": df.to_dict(orient="records")}
>>>>>>> c6acce87d3fc6e39a88cccaa4e37b7a71d923f4e
