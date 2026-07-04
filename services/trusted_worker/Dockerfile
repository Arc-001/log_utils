FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app

COPY services/trusted_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/trusted_worker/multiline.py .
COPY services/trusted_worker/main.py .

CMD ["python", "main.py"]
