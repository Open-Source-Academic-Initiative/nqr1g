"""
OpenSAI - SECOP Unified Query Microservice (I and II)
Migrated to FastAPI - Version 3.0.0
Date: February 2026
"""

from opensai_app import app

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
