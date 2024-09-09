#!/usr/bin/env python3
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

from influxdb_client import InfluxDBClient
import paho.mqtt.client as mqtt
import json
import time

# InfluxDB-Konfiguration
influxdb_url = "http://localhost:8086"  # URL der InfluxDB
influxdb_token = "YOUR_INFLUXDB_TOKEN"  # InfluxDB API Token
influxdb_org = "enpal"               # Organisation

# MQTT-Konfiguration
mqtt_broker = "MQTT_SERVER"  # MQTT Broker URL
mqtt_port = 1883                  # MQTT Broker Port
mqtt_topic = "enpal"      # MQTT Topic, auf das die Daten gesendet werden sollen

# Intervall für den zyklischen Abruf (in Sekunden)
interval = 60  # Beispiel: alle 60 Sekunden

# MQTT-Client erstellen
mqtt_client = mqtt.Client()


def get_tables(influx_server: str, influx_token: str, influx_org: str):
    client = InfluxDBClient(url=influx_server, token=influx_token, org=influx_org)
    query_api = client.query_api()

    query = 'from(bucket: "solar") \
      |> range(start: -5m) \
      |> last()'

    tables = query_api.query(query)
    return tables

def send_data_to_mqtt(data):
    mqtt_client.connect(mqtt_broker, mqtt_port, 60)
    mqtt_client.loop_start()
    
    # Daten als JSON serialisieren
    payload = json.dumps(data)
    
    # MQTT-Nachricht senden
    mqtt_client.publish(mqtt_topic, payload)
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

def convert_values(measurement, field, value):
    data_line = {}
    data_line[f'category'] = measurement
    data_line[f'field'] = field
    data_line[f'value'] = round(float(value), 2)
    return data_line


# Hauptschleife für den zyklischen Abruf und Versand
try:
    while True:
        result = get_tables(influxdb_url, influxdb_token, influxdb_org)

        data = []

        # Daten verarbeiten
        for table in result:
            field = table.records[0].values['_field']
            measurement = table.records[0].values['_measurement']
            value = table.records[0].values['_value']

            data.append(convert_values(measurement, field, value))

        # Daten an MQTT-Broker senden
        send_data_to_mqtt(data)

        # Wartezeit vor dem nächsten Zyklus
        time.sleep(interval)

except KeyboardInterrupt:
    print("Programm wurde beendet.")

# InfluxDB-Client schließen
client.close()