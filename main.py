import uvicorn

from event_pipeline.api import create_default_app

app = create_default_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
