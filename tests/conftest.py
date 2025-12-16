"""Root pytest configuration for all tests.

Loads .env file for local development. CI sets env vars directly.
"""
from pathlib import Path
from dotenv import load_dotenv


def pytest_configure(config):
    """Load environment variables from .env before running tests."""
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
