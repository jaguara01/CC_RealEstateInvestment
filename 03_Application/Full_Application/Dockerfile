# syntax=docker/dockerfile:1

# builder
FROM python:3.11-slim AS builder

RUN apt-get update && \
	apt-get install -y --no-install-recommends build-essential && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /install

COPY requirements.txt .

# Install into a private prefix
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /install /usr/local

COPY . .

RUN find /usr/local -type d \( -name test -o -name tests \) -exec rm -rf {} +

# expose streamlit’s default port
EXPOSE 80

ENV STREAMLIT_SERVER_HEADLESS=true \
	STREAMLIT_SERVER_PORT=80 \
	STREAMLIT_SERVER_ENABLECORS=false

# launch
CMD ["streamlit", "run", "--server.port=80", "main.py"]
