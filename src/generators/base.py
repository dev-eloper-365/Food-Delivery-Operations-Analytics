from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List
import pandas as pd
import numpy as np
from faker import Faker


@dataclass
class GeneratorConfig:
    """Configuration for data generators."""
    seed: int = 42
    null_rate: float = 0.02
    duplicate_rate: float = 0.005

    def __post_init__(self):
        np.random.seed(self.seed)
        Faker.seed(self.seed)


class BaseGenerator(ABC):
    """Abstract base class for all data generators."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.faker = Faker("en_IN")

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        pass

    def inject_nulls(self, df: pd.DataFrame, columns: List[str], rate: Optional[float] = None) -> pd.DataFrame:
        rate = rate or self.config.null_rate
        df = df.copy()
        for col in columns:
            if col in df.columns:
                mask = np.random.random(len(df)) < rate
                df.loc[mask, col] = None
        return df

    def inject_duplicates(self, df: pd.DataFrame, rate: Optional[float] = None) -> pd.DataFrame:
        rate = rate or self.config.duplicate_rate
        n_dups = int(len(df) * rate)
        if n_dups == 0:
            return df
        dup_indices = np.random.choice(df.index, n_dups, replace=True)
        duplicates = df.loc[dup_indices].copy()
        return pd.concat([df, duplicates], ignore_index=True)

    def save(self, df: pd.DataFrame, path: str, format: str = "csv") -> None:
        if format == "csv":
            df.to_csv(path, index=False)
        elif format == "json":
            df.to_json(path, orient="records", indent=2)
        else:
            raise ValueError(f"Unknown format: {format}")
