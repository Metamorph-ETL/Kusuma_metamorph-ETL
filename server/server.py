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
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


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