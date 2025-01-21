import uvicorn
import fastapi
import json
import libression.router.media_router


def create_app() -> fastapi.FastAPI:
    """
    Factory function to create the FastAPI application.
    This allows for more flexible app creation and testing.
    """
    app = fastapi.FastAPI(
        title="Libression API",
        description="Media management API",
        version="1.0.0",
        lifespan=libression.router.media_router.lifespan,
    )

    # Include the media router
    app.include_router(libression.router.media_router.router)

    # Optional: Add a health check endpoint
    @app.get("/health")
    async def health_check():
        return {"status": "healthy"}

    return app


# Create the app instance for direct running
app = create_app()

if __name__ == "__main__":

    # Update the openapi spec (should be gitignored)
    openapi_spec = app.openapi()
    with open("openapi.json", "w") as f:
        json.dump(openapi_spec, f, indent=4)
    
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
