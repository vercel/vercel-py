from fastapi import FastAPI

app = FastAPI()


@app.get("/api")
def read_root() -> dict[str, str]:
    return {"message": "hello world"}
