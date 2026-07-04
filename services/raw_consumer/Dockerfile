FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app

COPY services/raw_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/raw_consumer/main.py .

CMD ["python", "main.py"]
