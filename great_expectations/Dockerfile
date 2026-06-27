FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY validate_quality.py .
CMD ["python", "validate_quality.py"]