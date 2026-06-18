from datetime import datetime, timedelta
from jose import jwt, JWTError
from typing import Optional

import os
import re
import hashlib
import bcrypt
from dotenv import load_dotenv

load_dotenv()


SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def is_sha256(s: str) -> bool:
    return bool(isinstance(s, str) and len(s) == 64 and re.match(r'^[0-9a-fA-F]{64}$', s))

def hash_password(password: str) -> str:
    if is_sha256(password):
        sha_password = password
    else:
        sha_password = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return bcrypt.hashpw(sha_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    # Step 1: verify(client_hash, stored_hash)
    try:
        if bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8")):
            return True
    except Exception:
        pass

    # Step 2: verify(plain_password, stored_hash) legacy verification
    if not is_sha256(plain_password):
        try:
            sha_password = hashlib.sha256(plain_password.encode("utf-8")).hexdigest()
            if bcrypt.checkpw(sha_password.encode("utf-8"), hashed_password.encode("utf-8")):
                return True
        except Exception:
            pass

    return False

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    import time
    now_ts = int(time.time())
    expire_ts = now_ts + int((expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).total_seconds())
    to_encode.update({
        "exp": expire_ts,
        "iat": now_ts
    })
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_access_token(token: str):
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
