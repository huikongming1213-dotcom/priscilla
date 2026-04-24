# Build from priscilla/ root so `forms/` is in the build context:
#   cd priscilla/
#   docker build -t form-pdf-service -f form-pdf-service/Dockerfile .
#
# Zeabur: set Root Directory = priscilla/, Dockerfile Path = form-pdf-service/Dockerfile
FROM python:3.11-slim
WORKDIR /app

# 1. 複製 requirements 並安裝
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 2. 複製所有程式碼檔案 (.py)
COPY *.py ./

# 3. 複製 forms 資料夾 (內含 PDF 同 Mapping)
COPY forms/ ./forms/

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]