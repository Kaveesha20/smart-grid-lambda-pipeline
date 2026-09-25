import json
import logging
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "meter_producer", "msg": %(message)s}',
)
logger = logging.getLogger("meter_producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("METER_TOPIC", "meter_readings")
NUM_METERS = int(os.environ.get("NUM_METERS", "20"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "2"))
SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS", "300"))  # 5 real min = 1 sim day

ZONES = ["ZoneA_Fort", "ZoneB_Kollupitiya", "ZoneC_Dehiwala", "ZoneD_Rajagiriya"]


class Meter:
    def __init__(self, meter_id, household_id, zone):
        self.meter_id = meter_id
        self.household_id = household_id
        self.zone = zone
        self.has_solar = random.random() < 0.4  # 40% of households have solar panels
        self.base_load = random.uniform(0.3, 1.2)  # baseline kWh draw

    def step(self, hour_of_day):
        morning_peak = math.exp(-((hour_of_day - 7.5) ** 2) / 4) * 2.0
        evening_peak = math.exp(-((hour_of_day - 20) ** 2) / 6) * 2.5
        consumption = self.base_load + morning_peak + evening_peak
        consumption += random.uniform(-0.2, 0.2)
        consumption = max(0.05, consumption)

        solar = 0.0
        if self.has_solar and 6 <= hour_of_day <= 18:
            solar = math.exp(-((hour_of_day - 12) ** 2) / 8) * random.uniform(1.5, 3.0)
            solar = max(0.0, solar)

        READING_SCALE = 0.015
        consumption *= READING_SCALE
        solar *= READING_SCALE    

        return {
            "meter_id": self.meter_id,
            "household_id": self.household_id,
            "power_consumption_kwh": round(consumption, 3),
            "solar_generation_kwh": round(solar, 3),
            "grid_zone": self.zone,
        }


def sim_now():
    """Return a timestamp scaled so 24h passes every SIM_DAY_SECONDS real seconds."""
    scale = 86400.0 / SIM_DAY_SECONDS
    real_elapsed = time.time() - sim_now.start_real
    sim_elapsed = real_elapsed * scale
    return sim_now.start_wall + timedelta(seconds=sim_elapsed)


sim_now.start_real = time.time()
sim_now.start_wall = datetime.now(timezone.utc)


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
        linger_ms=50,
    )


def main():
    meters = [
        Meter(f"meter_{i:03d}", f"hh_{i:03d}", ZONES[i % len(ZONES)])
        for i in range(NUM_METERS)
    ]
    producer = build_producer()
    logger.info(json.dumps(f"starting meter producer: {NUM_METERS} meters, topic={TOPIC}"))

    sent = 0
    try:
        while True:
            current = sim_now()
            hour_of_day = current.hour + current.minute / 60.0
            for m in meters:
                event = m.step(hour_of_day)
                event["timestamp"] = current.isoformat()
                try:
                    producer.send(TOPIC, key=event["meter_id"], value=event)
                    sent += 1
                except KafkaError as e:
                    logger.error(json.dumps(f"failed to send event for {event['meter_id']}: {e}"))
            producer.flush()
            if sent % 100 < NUM_METERS:
                logger.info(json.dumps(f"heartbeat: {sent} events sent so far"))
            time.sleep(EMIT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info(json.dumps("shutting down meter producer"))
    finally:
        producer.close()


if __name__ == "__main__":
    main()
