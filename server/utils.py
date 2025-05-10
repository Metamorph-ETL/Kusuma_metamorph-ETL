from datetime import datetime, timedelta
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Optional
from key import key_path
import os

# key_Path 
GCS_CREDENTIALS_PATH = ['D:/ETL_pipeline_project/.env']
key_path = r"D:\ETL_pipeline_project\server\meta-morph-d-eng-pro-view-key.json"




# Secret key and algo
SECRET_KEY = "Project-Metamorph"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# OAuth2PasswordBearer instance
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# user
user_db ={
"admin":{
    "username": "admin",
    "password": "admin",
    "disabled": False
    }
}

# Pydantic model for token data
class TokenData(BaseModel):
    username: Optional[str] = None

# Pydantic model representing a basic user
class User(BaseModel):
    username: str
    disabled: Optional[bool] = None

# Pydantic model representing a user in the database (includes hashed_password)
class UserInDB(User):
    password: str

# Utility function to get a user object from the database by username
def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

# Function to authenticate the user by verifying username and password
def authenticate_user(db, username: str, password: str):
    user = get_user(db, username)
    if not user or user.password != password:
        return False
    return user

# Function to create JWT token
def create_access_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Dependency function that retrieves the current user based on the token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = get_user(user_db, username)
    if user is None:
        raise credentials_exception
    return user

# Dependency function that ensures the user is active (not disabled)
async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
