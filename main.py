import json
import random
import time
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict


class SensorType(Enum):
    HUMIDITY = "humidity"
    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    VIBRATION = "vibration"
    LIGHT = "light"
    MOTION = "motion"


@dataclass
class SensorReading:
    sensor_id: str
    sensor_type: SensorType
    value: float
    unit: str
    timestamp: float
    battery_level: float
    signal_strength: int


class IotSensor:
    def __init__(
        self,
        sensor_id: str,
        sensor_type: SensorType,
        unit: str,
        reading_range: tuple,
        noise_factor: float = 0.1,
    ):
        self.sensor_id: str = sensor_id
        self.sensor_type: SensorType = sensor_type
        self.unit: str = unit
        self.min_value: float = reading_range[0]
        self.max_value: float = reading_range[1]
        self.noise_factor: float = noise_factor
        self.battery_level: float = 100.0
        self.base_value: float = random.uniform(self.min_value, self.max_value)

    def get_reading(self) -> SensorReading:
        self.base_value += random.uniform(-self.noise_factor, self.noise_factor)
        self.base_value = max(min(self.base_value, self.max_value), self.min_value)
        self.battery_level -= random.uniform(0.01, 0.05)
        self.battery_level = max(0.0, self.battery_level)
        signal_strength = random.randint(-90, -30)

        return SensorReading(
            sensor_id=self.sensor_id,
            sensor_type=self.sensor_type,
            value=self.base_value,
            unit=self.unit,
            timestamp=time.time(),
            battery_level=self.battery_level,
            signal_strength=signal_strength,
        )


class KafkaSensorNetwork:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.sensors: Dict[str, IotSensor] = {}
        self.setup_sensor_network()

        # Initialize Kafka producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=str.encode,
        )

        # Define Kafka topics
        self.topics = {
            "temperature": "sensor.temperature",
            "humidity": "sensor.humidity",
            "light": "sensor.light",
            "motion": "sensor.motion",
            "alerts": "sensor.alerts",
        }

    def setup_sensor_network(self):
        # Temperature sensors (Celsius)
        for i in range(5):
            self.sensors[f"temp_{i}"] = IotSensor(
                f"temp_{i}", SensorType.TEMPERATURE, "°C", (18.0, 28.0), 0.2
            )

        # Humidity sensors (%)
        for i in range(3):
            self.sensors[f"hum_{i}"] = IotSensor(
                f"hum_{i}", SensorType.HUMIDITY, "%", (30.0, 70.0), 0.5
            )

        # Light sensors (lux)
        for i in range(2):
            self.sensors[f"light_{i}"] = IotSensor(
                f"light_{i}", SensorType.LIGHT, "lux", (0.0, 1000.0), 10.0
            )

        # Motion sensors (binary with noise)
        for i in range(2):
            self.sensors[f"motion_{i}"] = IotSensor(
                f"motion_{i}", SensorType.MOTION, "binary", (0.0, 1.0), 0.1
            )

    def publish_reading(self, reading: SensorReading):
        try:
            # Create message payload
            message = {
                "sensor_id": reading.sensor_id,
                "type": reading.sensor_type.value,
                "value": round(reading.value, 2),
                "unit": reading.unit,
                "timestamp": datetime.fromtimestamp(reading.timestamp).isoformat(),
                "battery": round(reading.battery_level, 1),
                "signal": reading.signal_strength,
            }

            # Publish to type-specific topic
            topic = self.topics.get(reading.sensor_type.value, "sensor.unknown")
            future = self.producer.send(topic, key=reading.sensor_id, value=message)
            # Wait for message to be sent
            future.get(timeout=10)
            print(f"Published to {topic}: {message}")

        except KafkaError as e:
            print(f"Error publishing message: {e}")

    def run(self):
        try:
            while True:
                for sensor in self.sensors.values():
                    reading = sensor.get_reading()
                    self.publish_reading(reading)
                time.sleep(2)  # Simulate sensor reading interval

        except KeyboardInterrupt:
            print("Shutting down sensor network...")
            self.producer.close()


class AlertMonitor:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="alert_monitor_group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )

        # Subscribe to all sensor topics
        self.consumer.subscribe(pattern="sensor.*")

        # Alert thresholds
        self.thresholds = {
            "temperature": 26.0,  # °C
            "humidity": 65.0,  # %
            "battery": 20.0,  # %
            "signal": -80,  # dBm
        }

        # Initialize alert producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def check_alerts(self, message):
        alerts = []
        sensor_type = message["type"]

        # Check value thresholds
        if sensor_type in self.thresholds:
            if message["value"] > self.thresholds[sensor_type]:
                alerts.append(
                    f"HIGH {sensor_type.upper()}: {message['sensor_id']} at {message['value']}{message['unit']}"
                )

        # Check battery level
        if message["battery"] < self.thresholds["battery"]:
            alerts.append(
                f"LOW BATTERY: {message['sensor_id']} at {message['battery']}%"
            )

        # Check signal strength
        if message["signal"] < self.thresholds["signal"]:
            alerts.append(
                f"POOR SIGNAL: {message['sensor_id']} at {message['signal']} dBm"
            )

        return alerts

    def run(self):
        try:
            for message in self.consumer:
                data = message.value
                alerts = self.check_alerts(data)

                for alert in alerts:
                    self.producer.send(
                        "sensor.alerts",
                        {
                            "timestamp": datetime.now().isoformat(),
                            "alert": alert,
                            "sensor_data": data,
                        },
                    )
                    print(f"ALERT: {alert}")

        except KeyboardInterrupt:
            print("Shutting down alert monitor...")
            self.consumer.close()
            self.producer.close()


class DataAggregator:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id="aggregator_group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
        )

        self.consumer.subscribe(pattern="sensor.*")
        self.readings = {"temperature": [], "humidity": [], "light": [], "motion": []}

    def calculate_statistics(self):
        for sensor_type, readings in self.readings.items():
            if readings:
                values = [r["value"] for r in readings]
                avg = sum(values) / len(values)
                print(f"{sensor_type.capitalize()} Average: {avg:.2f}")

        # Clear readings for next window
        self.readings = {k: [] for k in self.readings.keys()}

    def run(self):
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in messages.items():
                    for message in messages:
                        data = message.value
                        sensor_type = data["type"]
                        if sensor_type in self.readings:
                            self.readings[sensor_type].append(data)

                # Calculate statistics every 10 seconds
                self.calculate_statistics()
                time.sleep(10)

        except KeyboardInterrupt:
            print("Shutting down data aggregator...")
            self.consumer.close()


def main():
    # Create and start sensor network
    network = KafkaSensorNetwork()
    alert_monitor = AlertMonitor()
    aggregator = DataAggregator()

    # Start each component in a separate thread
    network_thread = threading.Thread(target=network.run)
    alert_thread = threading.Thread(target=alert_monitor.run)
    aggregator_thread = threading.Thread(target=aggregator.run)

    try:
        print("Starting IoT Sensor Network...")
        network_thread.start()
        print("Starting Alert Monitor...")
        alert_thread.start()
        print("Starting Data Aggregator...")
        aggregator_thread.start()

        # Wait for all threads to complete
        network_thread.join()
        alert_thread.join()
        aggregator_thread.join()

    except KeyboardInterrupt:
        print("Shutting down all components...")


if __name__ == "__main__":
    main()
