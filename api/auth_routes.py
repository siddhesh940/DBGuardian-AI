from fastapi import APIRouter, Request, HTTPException, Response
from fastapi.datastructures import FormData
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
import hashlib
import uuid
import os
import json
from datetime import datetime

from starlette.datastructures import UploadFile

from starlette.datastructures import UploadFile

router = APIRouter()

# In-memory sessions (runtime)
sessions = {}

USERS_FILE = "data/users.json"
BASE_USER_DATA_DIR = "data/users"     # <<< PER USER DATA ROOT (STRICT ISOLATION)


# -------------------------
# Models
# -------------------------
class UserRegistration(BaseModel):
    username: str
    password: str


class UserLogin(BaseModel):
    username: str
    password: str


# -------------------------
# Helpers
# -------------------------
def hash_password(password) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def load_users():
    if not os.path.exists(USERS_FILE):
        return {}

    with open(USERS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_users(users) -> None:
    os.makedirs("data", exist_ok=True)
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2)


def ensure_user_workspace(username: str) -> str:
    """
    MAKE SURE ek user ka apna isolated storage ho.
    Sab RCA / Upload / Results yahin jayenge.
    """
    user_base: str = os.path.join(BASE_USER_DATA_DIR, username)

    paths: list[str] = [
        user_base,
        os.path.join(user_base, "parsed_csv"),
        os.path.join(user_base, "results"),
        os.path.join(user_base, "raw_html")
    ]

    for p in paths:
        os.makedirs(p, exist_ok=True)

    return user_base


# -------------------------
# Load Users
# -------------------------
users_db = load_users()

# Create default admin (first time only)
if "admin" not in users_db:
    users_db["admin"] = {
        "password": hash_password("admin123"),
        "created_at": str(datetime.now())
    }
    save_users(users_db)
    print("Created default user: admin / admin123")


# -------------------------
# Routes
# -------------------------
@router.post("/register")
async def register(user_data: UserRegistration) -> JSONResponse:
    username: str = user_data.username.strip()

    if username in users_db:
        raise HTTPException(status_code=400, detail="Username already exists")

    users_db[username] = {
        "password": hash_password(user_data.password),
        "created_at": str(datetime.now())
    }

    save_users(users_db)

    # create personal workspace
    ensure_user_workspace(username)

    return JSONResponse({"message": "Registration successful"})


@router.post("/login")
async def login(user_data: UserLogin) -> JSONResponse:
    username: str = user_data.username.strip()
    user = users_db.get(username)

    if not user or user["password"] != hash_password(user_data.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session_id = str(uuid.uuid4())

    sessions[session_id] = {
        "username": username,
        "login_time": str(datetime.now())
    }

    # ensure workspace always exists
    ensure_user_workspace(username)

    # IMPORTANT: Set cookie on the SAME JSONResponse object we return
    resp = JSONResponse({
        "success": True,
        "message": "Login successful",
        "redirect": "/dashboard",
        "session_id": session_id,
        "username": username
    })
    resp.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,       # more secure
        max_age=86400,       # 24 hours
        samesite="lax",
        secure=False,        # Set to True in production with HTTPS
        path="/"             # Important: Cookie available for all routes
    )
    return resp


@router.post("/simple-login")
async def simple_login(request: Request) -> RedirectResponse:
    form: FormData = await request.form()
    username: UploadFile | str | None = form.get("username")
    password: UploadFile | str | None = form.get("password")

    if not username or not password:
        return RedirectResponse(url="/login?error=missing_credentials", status_code=302)

    user = users_db.get(username)
    if not user or user["password"] != hash_password(password):
        return RedirectResponse(url="/login?error=invalid_credentials", status_code=302)

    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        "username": username,
        "login_time": str(datetime.now())
    }

    response = RedirectResponse(url="/dashboard", status_code=302)
    response.set_cookie(
        key="session_id",
        value=session_id,
        httponly=True,
        max_age=86400,
        samesite="lax",
        secure=False,
        path="/"
    )

    ensure_user_workspace(username)

    return response


@router.post("/logout")
async def logout(request: Request, response: Response) -> JSONResponse:
    session_id: str | None = request.cookies.get("session_id")

    if session_id and session_id in sessions:
        del sessions[session_id]

    response.delete_cookie("session_id")

    return JSONResponse({
        "message": "Logged out successfully",
        "redirect": "/login"
    })

