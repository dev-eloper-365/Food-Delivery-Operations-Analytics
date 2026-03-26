import numpy as np
import pandas as pd
from datetime import date, timedelta
from .base import BaseGenerator, GeneratorConfig


class RestaurantsGenerator(BaseGenerator):
    """Generate synthetic restaurant data."""

    def __init__(self, config: GeneratorConfig, data_config: dict):
        super().__init__(config)
        self.data_config = data_config
        self.n_restaurants = data_config["volumes"]["restaurants"]
        self.cities = data_config["volumes"]["cities"]
        self.cuisine_types = [
            "North Indian", "South Indian", "Chinese", "Italian",
            "Fast Food", "Biryani", "Street Food", "Desserts",
        ]
        self.rating_bands = ["A", "B", "C", "D"]
        self.rating_weights = [0.2, 0.4, 0.3, 0.1]

    def generate(self) -> pd.DataFrame:
        n = self.n_restaurants

        # Generate restaurant IDs
        ids = [f"REST-{np.random.randint(10000, 99999):05d}" for _ in range(n)]

        # Generate onboarding dates between 2024-01-01 and 2026-01-15
        start = date(2024, 1, 1)
        end = date(2026, 1, 15)
        days_range = (end - start).days
        onboarding_dates = [
            start + timedelta(days=int(np.random.randint(0, days_range)))
            for _ in range(n)
        ]

        df = pd.DataFrame({
            "restaurant_id": ids,
            "name": [self.faker.company() for _ in range(n)],
            "city": np.random.choice(self.cities, n),
            "cuisine_type": np.random.choice(self.cuisine_types, n),
            "rating_band": np.random.choice(
                self.rating_bands, n, p=self.rating_weights
            ),
            "onboarding_date": onboarding_dates,
        })

        # Inject nulls
        df = self.inject_nulls(df, ["cuisine_type", "rating_band"])

        # Inject duplicates
        df = self.inject_duplicates(df)

        return df
