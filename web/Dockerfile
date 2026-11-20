FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --disable-pip-version-check --no-cache-dir -r requirements.txt
COPY manage.py ./
COPY cartsite ./cartsite
COPY store ./store
COPY templates ./templates
COPY static ./static
USER nobody
CMD ["sh", "-c", "python manage.py migrate --noinput && exec python manage.py runserver 0.0.0.0:8000 --noreload"]
