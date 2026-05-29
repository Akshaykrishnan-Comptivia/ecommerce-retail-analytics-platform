import yaml
from faker import Faker
from random import choice
import uuid
from datetime import datetime

fake = Faker()

DEVICE_TYPES = ["mobile", "desktop", "tablet"]


class SyntheticDataGenerator:
    def __init__(self, spark, config_path=None):
        self.spark = spark
        self.config_path = config_path

        if config_path:
            with open(config_path, encoding="utf-8") as f:
                   self.config = yaml.safe_load(f)

        self.landing_zone = self.config["storage"]["raw_landing_zone"]

        self.clickstream_cfg = self.config["synthetic"]["clickstream"]
        
    def _make_event(
    self,
    session_id,
    user_id,
    event_type,
    product_id,
    timestamp
):
         return {
            "event_id":str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "event_type": event_type,
            "product_id": product_id,
            "timestamp": timestamp.isoformat(),
            "device": choice(DEVICE_TYPES),
               "referrer": fake.url(),
        }
    def generate_clickstream_events(self):
         events = []
         num_events = self.clickstream_cfg["num_events"]
         while len(events) < num_events:

            session_id = f"sess_{choice(range(1, self.clickstream_cfg['num_sessions'] + 1))}"
            user_id = f"user_{choice(range(1, self.clickstream_cfg['num_users'] + 1))}"
            product_id = f"prod_{choice(range(1, self.clickstream_cfg['num_products'] + 1))}"
            event_type = choice(self.clickstream_cfg["event_types"])
            timestamp = datetime.now()
            event = self._make_event(
                    session_id,
                    user_id,
                    event_type,
                    product_id,
                    timestamp
            )

            events.append(event)
        return events