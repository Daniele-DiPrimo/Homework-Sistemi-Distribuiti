#LABEL author="MatteoSecondo"
# Dockerfile for a simple Python application"
FROM python:3.14-slim
WORKDIR /src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "src/app.py"]
#ENTRYPOINT ["python", "app.py"]