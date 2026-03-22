from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db import init_db
from app.routes import auth

app = FastAPI(title="TradeAI API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "[http://127.0.0.1:3000](http://127.0.0.1:3000)"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Database on Startup
@app.on_event("startup")
async def start_db():
    await init_db()

@app.get("/")
async def root():
    return {"message": "Backend is running!"}

# Include Routes
app.include_router(auth.router, prefix="/api", tags=["Authentication"])