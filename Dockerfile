FROM python:alpine

ADD enpal2mqtt.py /
ADD enpal2mqtt.cfg_demo /enpal2mqtt.cfg

RUN pip install paho-mqtt influxdb-client

RUN mkdir -p /certs

CMD [ "python", "/enpal2mqtt.py", "/enpal2mqtt.cfg" ]