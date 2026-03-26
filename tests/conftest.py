import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


@pytest.fixture
def project_root():
    return Path(__file__).parent.parent.parent


@pytest.fixture
def raw_data_path(project_root):
    return project_root / "data" / "raw"
