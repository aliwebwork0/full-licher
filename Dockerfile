FROM alpine:latest

RUN apk add --no-cache \
    python3 \
    py3-pip \
    curl \
    rclone \
    bash

WORKDIR /app

COPY requirements.txt .
RUN pip install --break-system-packages -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["./start.sh"]
