FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app

COPY services/refined_loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/refined_loader/main.py .

CMD ["python", "main.py"]
