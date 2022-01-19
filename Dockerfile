FROM python:3.8-slim as base

RUN pip install poetry --quiet

COPY airbyte-connector/pyproject.toml .
COPY airbyte-connector/poetry.lock .

#Installing environment variables
ENV LANG C.UTF-8
ENV PATH="${PATH}:/root/.poetry/bin"
RUN python -m venv /app/venv
RUN . /app/venv/bin/activate && poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-dev

FROM python:3.8-slim as final

#set application name
ARG APP_NAME=airbyte_connector

COPY --from=base /app/venv /app/venv/
ENV PATH /app/venv/bin:$PATH

RUN mkdir -p /root/${APP_NAME}/
WORKDIR /root/${APP_NAME}/
 
COPY airbyte-connector/src/ src/

ENTRYPOINT ["python", "src/main.py"]
