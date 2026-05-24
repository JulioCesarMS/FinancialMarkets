# Imagen base ligera de Python
FROM python:3.12-slim

# Evita archivos .pyc y buffering
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias del sistema (opcional, útil para pandas, lxml, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primero (para aprovechar cache)
COPY requirements.txt /app/

# Instalar dependencias Python
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar todo el proyecto
COPY . /app/

# Agregar src al PYTHONPATH (muy importante para imports)
ENV PYTHONPATH=/app/src

# Comando por defecto (ajústalo a tu entrypoint)
CMD ["python", "main.py"]