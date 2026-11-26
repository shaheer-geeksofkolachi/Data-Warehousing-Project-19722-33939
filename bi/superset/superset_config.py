"""Apache Superset configuration for the SDI BI layer (local demo stack)."""
import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "sdi-phase1-bi-demo-secret-key-change-me")
SQLALCHEMY_DATABASE_URI = os.getenv("SUPERSET_METADATA_URI",
                                    "postgresql+psycopg2://postgres:postgres@postgres:5432/superset")

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,      # click a bar / slice to filter the rest of the dashboard
    "DRILL_TO_DETAIL": True,              # right-click -> see the underlying rows
    "DRILL_BY": True,                     # right-click -> drill by another dimension
    "HORIZONTAL_FILTER_BAR": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_VIRTUALIZATION": False,    # render every chart (screenshots / demo)
}

# Dashboards must show the newest refresh immediately: no result caching (BI tables are small pre-aggregates,
# so live queries take milliseconds). Dashboards also auto-refresh every 30 s (json_metadata.refresh_frequency).
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 86400}
EXPLORE_FORM_DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 86400}

# local demo stack: no TLS / CSP hardening, CSRF off for the provisioning API client
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False
ENABLE_PROXY_FIX = True
SQLLAB_CTAS_NO_LIMIT = True
ROW_LIMIT = 50000
SUPERSET_WEBSERVER_TIMEOUT = 120
APP_NAME = "SDI BI — Sales & Demand Intelligence"
