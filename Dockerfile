FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 仅在依赖变化时重新安装
COPY requirements.txt .
RUN pip install -r requirements.txt

# 非 root 运行
RUN useradd --create-home --uid 10001 appuser
COPY app ./app
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# slim 镜像没有 curl，用标准库做健康检查
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
