import numpy as np
import pandas as pd
from datetime import date, timedelta
from .base import BaseGenerator, GeneratorConfig


class RestaurantsGenerator(BaseGenerator):
    """Generate synthetic restaurant data."""

    CUISINES = ["North Indian", "South Indian", "Chinese", "Italian",
                "Fast Food", "Biryani", "Street Food", "Desserts"]
    RATING_BANDS = ["A", "B", "C", "D"]
    RATING_WEIGHTS = [0.2, 0.4, 0.3, 0.1]

    def __init__(self, config: GeneratorConfig, data_config: dict):
        super().__init__(config)
        self.data_config = data_config
        self.n_restaurants = data_config["volumes"]["restaurants"]
        self.cities = data_config["volumes"]["cities"]

    def generate(self) -> pd.DataFrame:
        records = []
        for i in range(self.n_restaurants):
            city = self.cities[i % len(self.cities)]
            onboard_start = date(2024, 1, 1)
            onboard_end = date(2026, 1, 15)
            days_range = (onboard_end - onboard_start).days
            onboarding = onboard_start + timedelta(days=np.random.randint(0, days_range))

            records.append({
                "restaurant_id": f"REST-{np.random.randint(10000, 99999)}",
                "city": city,
                "cuisine_type": np.random.choice(self.CUISINES),
                "rating_band": np.random.choice(self.RATING_BANDS, p=self.RATING_WEIGHTS),
                "onboarding_date": onboarding.isoformat(),
            })

        df = pd.DataFrame(records)
        df = self.inject_nulls(df, ["cuisine_type", "rating_band"])
        return df
