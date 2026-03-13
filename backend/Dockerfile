FROM python:3.11-slim

WORKDIR /app

# Install deps (cached layer — context is repo root)
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy backend package
COPY backend/ ./backend/

# Create a non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8000

# Railway injects $PORT — fall back to 8000 for local dev
CMD ["sh", "-c", "python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000} --loop uvloop --http h11 --log-level info"]
