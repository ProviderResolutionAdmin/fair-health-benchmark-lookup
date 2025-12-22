from fastapi import FastAPI

app = FastAPI(title="TX FH Allowed Medical Lookup")

@app.get("/")
def health_check():
    return {"status": "ok"}
