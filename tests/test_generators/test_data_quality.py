import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.generators.base import GeneratorConfig
from src.generators.restaurants import RestaurantsGenerator
from src.generators.riders import RidersGenerator
from src.generators.orders import OrdersGenerator
from config.settings import DATA_CONFIG


class TestRestaurantsGenerator:
    def test_generates_correct_count(self):
        config = GeneratorConfig(seed=42)
        generator = RestaurantsGenerator(config, DATA_CONFIG)
        df = generator.generate()
        assert len(df) == DATA_CONFIG["volumes"]["restaurants"]

    def test_has_required_columns(self):
        config = GeneratorConfig(seed=42)
        generator = RestaurantsGenerator(config, DATA_CONFIG)
        df = generator.generate()
        expected_cols = ["restaurant_id", "city", "cuisine_type", "rating_band", "onboarding_date"]
        for col in expected_cols:
            assert col in df.columns

    def test_cities_are_valid(self):
        config = GeneratorConfig(seed=42)
        generator = RestaurantsGenerator(config, DATA_CONFIG)
        df = generator.generate()
        valid_cities = DATA_CONFIG["volumes"]["cities"]
        assert df["city"].dropna().isin(valid_cities).all()


class TestRidersGenerator:
    def test_generates_correct_count(self):
        config = GeneratorConfig(seed=42)
        generator = RidersGenerator(config, DATA_CONFIG)
        df = generator.generate()
        assert len(df) == DATA_CONFIG["volumes"]["riders"]

    def test_shift_types_are_valid(self):
        config = GeneratorConfig(seed=42)
        generator = RidersGenerator(config, DATA_CONFIG)
        df = generator.generate()
        valid_shifts = ["morning", "evening", "night"]
        assert df["shift_type"].dropna().isin(valid_shifts).all()


class TestOrdersGenerator:
    def test_generates_orders(self):
        config = GeneratorConfig(seed=42)
        restaurants = RestaurantsGenerator(config, DATA_CONFIG).generate()
        generator = OrdersGenerator(
            config=config,
            restaurants_df=restaurants,
            date_range=(DATA_CONFIG["date_range"]["start"], DATA_CONFIG["date_range"]["end"]),
            cities=DATA_CONFIG["volumes"]["cities"],
        )
        df = generator.generate()
        assert len(df) > 0

    def test_order_ids_unique(self):
        config = GeneratorConfig(seed=42)
        restaurants = RestaurantsGenerator(config, DATA_CONFIG).generate()
        generator = OrdersGenerator(
            config=config,
            restaurants_df=restaurants,
            date_range=(DATA_CONFIG["date_range"]["start"], DATA_CONFIG["date_range"]["end"]),
            cities=DATA_CONFIG["volumes"]["cities"],
        )
        df = generator.generate()
        # Account for injected duplicates
        assert df["order_id"].nunique() >= len(df) * 0.99

    def test_order_values_positive(self):
        config = GeneratorConfig(seed=42)
        restaurants = RestaurantsGenerator(config, DATA_CONFIG).generate()
        generator = OrdersGenerator(
            config=config,
            restaurants_df=restaurants,
            date_range=(DATA_CONFIG["date_range"]["start"], DATA_CONFIG["date_range"]["end"]),
            cities=DATA_CONFIG["volumes"]["cities"],
        )
        df = generator.generate()
        assert (df["order_value"] > 0).all()
