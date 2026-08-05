import os

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}"
    f"@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}"
    f"/{os.getenv('DATABASE_DB')}"
)

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "psx_pulse_superset_secret")