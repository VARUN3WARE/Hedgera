import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.src.api.main import app
from backend.config.settings import settings

if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 60)
    print("🚀 Starting AEGIS Trading System API Server")
    print("=" * 60)
    print(f"\n📡 API will be available at:")
    print(f"   • http://{settings.api_host}:{settings.api_port}")
    print(f"   • Docs: http://{settings.api_host}:{settings.api_port}/docs")
    print(f"\n💡 To start the full pipeline, use: python run_pipeline.py")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        "backend.src.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level="info"
    )

