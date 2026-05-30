import pytest
from sqlalchemy.engine import URL

from config.settings import settings


def test_database_url_masking():
    """Verify that the DATABASE_URL masks the password when stringified."""
    db_url = settings.DATABASE_URL

    # Ensure it's a URL object
    assert isinstance(db_url, URL)

    # Ensure string representation masks the password
    url_str = str(db_url)
    assert "***" in url_str
    assert settings.DATABASE_PASSWORD.get_secret_value() not in url_str

    # Ensure repr also masks the password
    url_repr = repr(db_url)
    assert "***" in url_repr
    assert settings.DATABASE_PASSWORD.get_secret_value() not in url_repr


def test_database_url_contains_components():
    """Verify that the DATABASE_URL contains necessary components."""
    db_url = settings.DATABASE_URL
    url_str = str(db_url)

    assert settings.DATABASE_USER in url_str
    assert settings.DATABASE_HOST in url_str
    assert str(settings.DATABASE_PORT) in url_str
    assert settings.DATABASE_NAME in url_str
    assert "ssl=" in url_str
