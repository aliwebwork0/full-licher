FROM python:3.12-slim

# system deps: rclone + yt-dlp + curl + ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip ffmpeg ca-certificates && \
    # rclone
    curl -fsSL https://rclone.org/install.sh | bash && \
    # yt-dlp
    curl -fsSL https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod +x /usr/local/bin/yt-dlp && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

EXPOSE 8080

CMD ["./start.sh"]
