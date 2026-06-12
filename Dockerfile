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

# curl-impersonate (Chrome build) — used for direct downloads to mimic
# a real Chrome TLS/HTTP2 fingerprint and avoid CDN bot-blocking
RUN curl -L https://github.com/lwthiker/curl-impersonate/releases/latest/download/curl-impersonate-v0.6.1.x86_64-linux-gnu.tar.gz \
    -o /tmp/curl-impersonate.tar.gz \
    && mkdir -p /opt/curl-impersonate \
    && tar -xzf /tmp/curl-impersonate.tar.gz -C /opt/curl-impersonate \
    && rm /tmp/curl-impersonate.tar.gz \
    && ln -sf /opt/curl-impersonate/curl_chrome116 /usr/local/bin/curl_chrome116 \
    && chmod +x /opt/curl-impersonate/curl_chrome116 || true

WORKDIR /app

COPY requirements.txt .
RUN pip install --break-system-packages -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["./start.sh"]
