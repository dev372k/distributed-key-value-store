# ---------------- Builder Stage ----------------
FROM python:3.11-slim AS builder

WORKDIR /install

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY main/requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------------- Final Stage ----------------
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy installed packages
COPY --from=builder /install /usr/local

# Copy application
COPY main/app.py .

# Persistent storage
RUN mkdir -p /app/data

EXPOSE 3030

# Multi-worker async server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3030"]
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3030", "--workers", "8"]