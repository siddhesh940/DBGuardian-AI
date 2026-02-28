from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from api.rca_routes import router as rca_router
from api.auth_routes import router as auth_router, sessions

# -------------------------------------------------
# APP INIT
# -------------------------------------------------
app = FastAPI(title="SQL Workload RCA System")

# -------------------------------------------------
# CORS (UI + API Safe)
# -------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # UI runs on same host
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# STATIC FILES
# -------------------------------------------------
app.mount("/static", StaticFiles(directory="ui/static"), name="static")

# Prevent browser caching of JS/CSS during development
@app.middleware("http")
async def no_cache_static(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/static/") and request.url.path.endswith((".js", ".css")):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# -------------------------------------------------
# ROUTERS
# -------------------------------------------------
app.include_router(auth_router, prefix="/auth")
app.include_router(rca_router, prefix="/api")

# -------------------------------------------------
# AUTH HELPER
# -------------------------------------------------
def get_current_user(request: Request):
    session_id = request.cookies.get("session_id")

    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")

    return sessions[session_id]

# -------------------------------------------------
# UI ROUTES
# -------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def root():
    with open("ui/templates/landing.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/login", response_class=HTMLResponse)
def login_page():
    with open("ui/templates/login.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/register", response_class=HTMLResponse)
def register_page():
    with open("ui/templates/register.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    try:
        get_current_user(request)
        with open("ui/templates/dashboard_new.html", "r", encoding="utf-8") as f:
            return f.read()
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

@app.get("/results", response_class=HTMLResponse)
def results(request: Request):
    try:
        get_current_user(request)
        with open("ui/templates/results.html", "r", encoding="utf-8") as f:
            return f.read()
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

@app.get("/newresults", response_class=HTMLResponse)
def newresults(request: Request):
    try:
        get_current_user(request)
        with open("ui/templates/newresults.html", "r", encoding="utf-8") as f:
            return f.read()
    except HTTPException:
        return RedirectResponse(url="/login", status_code=302)

# -------------------------------------------------
# HEALTH CHECK
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -------------------------------------------------
# LOCAL DEV RUN
# -------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=4539,
        reload=True
    )

