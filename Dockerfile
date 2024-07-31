FROM python:3.9

WORKDIR /app

COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/

COPY /app/entrypoint.sh /app/

ENTRYPOINT ["sh", "/app/entrypoint.sh"]

RUN chmod +x /app/entrypoint.sh

