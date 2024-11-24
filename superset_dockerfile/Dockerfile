FROM apache/superset:latest

USER root

# Обновление pip
RUN pip install --upgrade pip

# Установка clickhouse коннектора
RUN pip install clickhouse-connect

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    pkg-config && \
    rm -rf /var/lib/apt/lists/*  # Очистка кэша apt

# Установка Python-пакетов
RUN pip install mysqlclient

ENV ADMIN_USERNAME=${ADMIN_USERNAME}
ENV ADMIN_EMAIL=${ADMIN_EMAIL}
ENV ADMIN_PASSWORD=${ADMIN_PASSWORD}
ENV EXECUTOR=Local

COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/
COPY superset.db /app/superset_home/superset.db
ENV SUPERSET_LOAD_EXAMPLES=no
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT [ "/superset-init.sh" ]
