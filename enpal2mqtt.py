#!/usr/bin/env python3
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import os
import sys
import time
import numbers
import logging
import argparse
import configparser
from influxdb_client import InfluxDBClient
import paho.mqtt.client as mqtt

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description='''glues between enpal and mqtt stuff''')
parser.add_argument('config_file', metavar="<config_file>", help="file with configuration")
args = parser.parse_args()

# read and parse config file
config = configparser.RawConfigParser()
config.read(args.config_file)

# [mqtt]
MQTT_HOST = config.get("mqtt", "host")
MQTT_PORT = config.getint("mqtt", "port", fallback=1883)
MQTT_LOGIN = config.get("mqtt", "login", fallback=None)
MQTT_PASSWORD = config.get("mqtt", "password", fallback=None)
ROOT_TOPIC = config.get("mqtt", "roottopic")

# [enpal] - InfluxDB-Konfiguration
ENPAL_URL = config.get("enpal", "url")
ENPAL_ORG = config.get("enpal", "org", fallback="enpal")
ENPAL_TOKEN = config.get("enpal", "token")

# [log]
VERBOSE = config.get("log", "verbose")
LOGFILE = config.get("log", "logfile", fallback=None)

# Intervall für den zyklischen Abruf (in Sekunden)
INTERVAL = config.get("mqtt", "refresh", fallback=60)

APPNAME = "enpal2mqtt"

# init logging 
LOGFORMAT = '%(asctime)-15s %(message)s'

if VERBOSE:
    loglevel = logging.DEBUG
else:
    loglevel = logging.INFO

if LOGFILE:
    logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=loglevel)
else:
    logging.basicConfig(stream=sys.stdout, format=LOGFORMAT, level=loglevel)

logging.info("Starting " + APPNAME)
if VERBOSE:
    logging.info("DEBUG MODE")
else:
    logging.debug("INFO MODE")

# MQTT-Client erstellen
MQTT_CLIENT_ID = APPNAME + "_%d" % os.getpid()
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, MQTT_CLIENT_ID)

def get_tables():
    client = InfluxDBClient(url=ENPAL_URL, token=ENPAL_TOKEN, org=ENPAL_ORG)
    query_api = client.query_api()

    query = 'from(bucket: "solar") \
      |> range(start: -5m) \
      |> last()'
    
    logging.debug("Fetching data from Enpal Home")

    tables = query_api.query(query)
    client.close()

    logging.debug("Fetched %s datasets" % len(tables))

    return tables

def is_number(value):
    return isinstance(value, numbers.Number)

def send_data_to_mqtt(data):
    if MQTT_LOGIN:
        mqtt_client.username_pw_set(MQTT_LOGIN, MQTT_PASSWORD)
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.loop_start()

    logging.info("Sending data to MQTT...")
    logging.info("Sending %s datasets to MQTT" % len(data))
    
    for data_item in data:
        # MQTT-Nachricht senden
        topic = ROOT_TOPIC + "/" + data_item["category"] + "/" + data_item["field"].replace('.', '_')
        mqtt_client.publish(topic , data_item["value"])
        logging.debug("Sent data to topic %s" % topic)
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

def convert_values(measurement, field, value):
    data_line = {}
    data_line["category"] = measurement
    data_line["field"] = field
    data_line["value"] = round(float(value), 2) if (is_number(float)) else value
    return data_line

logging.debug("Enpal Url   : %s" % ENPAL_URL)
logging.debug("Enpal Org   : %s" % ENPAL_ORG)
logging.debug("MQTT broker : %s" % MQTT_HOST)
if MQTT_LOGIN:
    logging.debug("  port      : %s" % (str(MQTT_PORT)))
    logging.debug("  login     : %s" % MQTT_LOGIN)
logging.debug("roottopic   : %s" % ROOT_TOPIC)
logging.debug("Interval   : %s" % INTERVAL)



# Hauptschleife für den zyklischen Abruf und Versand
try:
    while True:
        result = get_tables()

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
        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("Programm wurde beendet.")