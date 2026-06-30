# 1. 베이스 이미지 (최적화된 Python 3.11)
FROM python:3.11-slim-bookworm

# 2. 시스템 패키지 + tzdata 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    tzdata \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 3. 타임존/KST 설정 (OS 레벨)
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 4. 런타임 환경
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

# 5. 작업 디렉토리
WORKDIR /app

# 6. 의존성 설치 (메모리 최적화)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --no-deps --find-links https://download.pytorch.org/whl/torch_stable.html -r requirements.txt || \
    pip install --no-cache-dir -r requirements.txt

# 7. 앱 소스 복사
COPY ./src ./src
COPY ./config ./config
COPY ./run_integrated_manager.py ./run_integrated_manager.py
COPY ./run_background_risk_manager.py ./run_background_risk_manager.py

# 8. output 디렉토리 생성 (루트 권한으로 실행)
RUN mkdir -p /app/output/cache && chmod 755 /app/output/cache

# 9. 엔트리포인트: 통합 매니저 실행
CMD ["python", "-u", "/app/run_integrated_manager.py"]
