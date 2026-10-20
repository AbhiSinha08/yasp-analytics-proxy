FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS source
WORKDIR /src
ADD --checksum=sha256:69ff4527188a26da42157c7fdfe12a1af8a73d80a0ca4da55ffd665b8ab6de08 https://codeload.github.com/nnnkkk7/snowflake-emulator/tar.gz/3ced3e8b0f2b69f3ff2c6141784f4faf9666ec11 /tmp/source.tar.gz
RUN tar -xzf /tmp/source.tar.gz --strip-components=1
COPY dev/snowflake/emulator-compat.go server/handlers/fixture_compat.go
COPY dev/snowflake/patch_emulator.py /tmp/patch_emulator.py
RUN python /tmp/patch_emulator.py

FROM golang:1.26-bookworm@sha256:ee3901c61e423732605a51f1b8157c849ec01194ea0a274213b5202b2c11e7bf AS builder
WORKDIR /src
COPY --from=source /src/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=1 go build -trimpath -buildvcs=false -ldflags="-s -w" -o /snowflake-emulator ./cmd/server

FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS server
ENV PYTHONUNBUFFERED=1 PORT=64616 DB_PATH=/data/YASP_DEMO.db STAGE_DIR=/data/stages
WORKDIR /app
COPY --from=builder /snowflake-emulator /app/snowflake-emulator
COPY dev/snowflake/server.py dev/snowflake/sql.py dev/snowflake/seed.sql ./
RUN mkdir /data && chown 10001:0 /data
USER 10001
EXPOSE 64616
ENTRYPOINT ["python", "server.py"]

FROM python:3.12.9-slim-bookworm@sha256:48a11b7ba705fd53bf15248d1f94d36c39549903c5d59edcfa2f3f84126e7b44 AS clients
RUN pip install --no-cache-dir --no-deps SQLAlchemy==1.4.54 psycopg2-binary==2.9.13 greenlet==3.1.1 sqlalchemy-redshift==0.8.14 packaging==26.3 setuptools==75.8.2
COPY tests/fixtures/clients/sqlalchemy_probe.py /app/probe.py
ENTRYPOINT ["python", "/app/probe.py"]
