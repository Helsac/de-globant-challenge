FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc

COPY requirements_api.txt .
RUN pip install --no-cache-dir -r requirements_api.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main_api:app", "--host", "0.0.0.0", "--port", "8000"]