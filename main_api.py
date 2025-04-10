from fastapi import FastAPI
from api.endpoints import router

app = FastAPI(
    title="Globant ETL API",
    description="API to upload CSVs and trigger an ETL process using Spark + MySQL.",
    version="1.0.0"
)

app.include_router(router)