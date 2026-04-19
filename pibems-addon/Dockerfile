ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY config /app/config
COPY rootfs /

RUN chmod a+x /etc/services.d/pibems/run
