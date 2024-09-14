# enpal2mqtt

_Hinweis_:
Das Projekt 'enpal2mqtt' ist ein unabhängiges Projekt und steht in keiner Verbindung mit dem Unternehmen Enpal.

enpal2mqtt liest die aktuellen InfluxDB Daten der Enpal Home Box und sendet alle Werte an einen MQTT-Broker.
Ziel ist es eine generische Lösung für jegliche Home Automation System zu bieten, die an MQTT angedockt werden können.

Die individuelle Konfiguration muss, basierend auf der Demo-Konfiguration, erstellt werden.

Die Konfiguration für die Influx-DB der Enpal Home Box findet man im Webinterface der Enpal Home unter Config und dort sind die verschiedenen Keys mit dem Präfix "Influx" zu finden.

Das Skript wird mit folgendem Shell Befehl gestartet:
```
python3 enpal2mqtt.py enpal2mqtt.cfg
```