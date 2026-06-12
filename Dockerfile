FROM alpine:latest

RUN apk add --no-cache \
    python3 \
    py3-pip \
    curl \
    rclone \
    bash \
    ffmpeg

# yt-dlp
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod +x /usr/local/bin/yt-dlp

WORKDIR /app

COPY requirements.txt .
RUN pip install --break-system-packages -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["./start.sh"]
