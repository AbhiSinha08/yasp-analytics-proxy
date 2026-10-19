FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS server
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=1
WORKDIR /app
COPY tests/support/snowflake/requirements.lock ./requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY tests/support/snowflake/fakesnow_*.py ./
COPY dev/snowflake/ ./
EXPOSE 64616
ENTRYPOINT ["python", "server.py"]

FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS clients
RUN pip install --no-cache-dir --no-deps SQLAlchemy==1.4.54 psycopg2-binary==2.9.13 greenlet==3.1.1 sqlalchemy-redshift==0.8.14 packaging==26.3 setuptools==75.8.2
COPY tests/fixtures/clients/sqlalchemy_probe.py /app/probe.py
ENTRYPOINT ["python", "/app/probe.py"]
