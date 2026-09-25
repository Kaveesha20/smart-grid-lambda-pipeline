

import csv
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "tariff_batch_source", "msg": "%(message)s"}',
)
logger = logging.getLogger("tariff_batch_source")

NUM_METERS = int(os.environ.get("NUM_METERS", "20"))
SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS", "300"))
OUTPUT_DIR = os.environ.get("TARIFF_OUTPUT_DIR", "/data/raw/tariffs")

BILLING_TIERS = ["residential_low", "residential_standard", "residential_high"]
TIER_RATES = {  
    "residential_low": (15.0, 25.0),
    "residential_standard": (25.0, 40.0),
    "residential_high": (40.0, 65.0),
}


def sim_now():
    scale = 86400.0 / SIM_DAY_SECONDS
    real_elapsed = time.time() - sim_now.start_real
    sim_elapsed = real_elapsed * scale
    return sim_now.start_wall + timedelta(seconds=sim_elapsed)


sim_now.start_real = time.time()
sim_now.start_wall = datetime.now(timezone.utc)


def generate_daily_file(sim_date_str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"tariffs_{sim_date_str}.csv")

    rows = []
    for i in range(NUM_METERS):
        household_id = f"hh_{i:03d}"
        tier = random.choice(BILLING_TIERS)
        low, high = TIER_RATES[tier]
        tariff_rate = round(random.uniform(low, high), 2)
        subsidy_flag = "Y" if tier == "residential_low" and random.random() < 0.3 else "N"
        rows.append(
            {
                "household_id": household_id,
                "tariff_rate": tariff_rate,
                "billing_tier": tier,
                "subsidy_flag": subsidy_flag,
            }
        )

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["household_id", "tariff_rate", "billing_tier", "subsidy_flag"]
        )
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f'"wrote daily tariff file: {filepath} ({len(rows)} rows)"')
    return filepath


def main():
    logger.info(
        f'"starting tariff batch source: 1 file every {SIM_DAY_SECONDS}s (simulated day), '
        f'output_dir={OUTPUT_DIR}"'
    )
    last_written_date = None
    try:
        while True:
            current = sim_now()
            sim_date_str = current.strftime("%Y-%m-%d")
            if sim_date_str != last_written_date:
                generate_daily_file(sim_date_str)
                last_written_date = sim_date_str
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info('"shutting down tariff batch source"')


if __name__ == "__main__":
    main()
