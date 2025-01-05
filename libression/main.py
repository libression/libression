import fastapi
import libression.router.media_router
import uvicorn


app = fastapi.FastAPI(lifespan=libression.router.media_router.lifespan)
app.include_router(libression.router.media_router.router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
