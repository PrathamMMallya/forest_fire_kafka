FROM python:3.10-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy inference requirements
COPY requirements.inference.txt .

RUN pip install --no-cache-dir -r requirements.inference.txt

# Copy inference code
COPY inference/ inference/
COPY artifacts/ artifacts/

EXPOSE 8000

CMD ["uvicorn", "inference.app:app", "--host", "0.0.0.0", "--port", "8000"]
