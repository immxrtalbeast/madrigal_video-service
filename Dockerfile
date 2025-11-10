FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates xz-utils libsndfile1 && \
    rm -rf /var/lib/apt/lists/* && \
    curl -sSL https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -o /tmp/ffmpeg.tar.xz && \
    tar -xJf /tmp/ffmpeg.tar.xz -C /tmp && \
    cp /tmp/ffmpeg-*-amd64-static/ffmpeg /usr/local/bin/ && \
    cp /tmp/ffmpeg-*-amd64-static/ffprobe /usr/local/bin/ && \
    rm -rf /tmp/ffmpeg.tar.xz /tmp/ffmpeg-*-amd64-static && \
    apt-get purge -y --auto-remove curl ca-certificates xz-utils && \
    rm -rf /var/lib/apt/lists/*

COPY vendor /tmp/vendor
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --no-index --find-links /tmp/vendor torch==2.2.1+cpu torchvision==0.17.1+cpu && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /tmp/vendor /tmp/requirements.txt
    
COPY models /root/.cache/whisper
COPY app /app/app

EXPOSE 8100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]
