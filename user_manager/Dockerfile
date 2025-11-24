FROM python:3.14

WORKDIR /app

# Copia e installa le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutto il codice (src)
COPY src/ src/

# Imposta il path per i moduli
ENV PYTHONPATH=/app/src

# Avvia il server
CMD ["python", "src/main.py"]