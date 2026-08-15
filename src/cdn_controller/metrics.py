from prometheus_client import Counter, Gauge

CONTROLLER_UP = Gauge("cdn_controller_up", "Controller process is running")
TARGET_HEALTHY = Gauge("cdn_target_healthy", "Target health", ["target"])
RESOURCE_STATUS = Gauge("cdn_resource_status", "Resource state encoded as a labelled gauge", ["target", "generation", "state"])
BYTES_SENT = Gauge("cdn_resource_bytes_sent_total", "Integrated bytes sent", ["target", "generation"])
TRAFFIC_RATIO = Gauge("cdn_resource_traffic_ratio", "Traffic divided by switch threshold", ["target", "generation"])
HTTP_STATUS = Gauge("cdn_resource_http_status", "Last root HTTP status", ["target"])
CERT_EXPIRY = Gauge("cdn_certificate_expiry_seconds", "Certificate seconds until expiry", ["target", "generation"])
ROTATION_STATE = Gauge("cdn_rotation_state", "Current state", ["target", "state"])
ROTATIONS = Counter("cdn_rotation_total", "Rotation attempts", ["target", "result"])
PROVIDER_ERRORS = Counter("cdn_provider_api_errors_total", "Provider API errors", ["provider"])
LAST_SUCCESS = Gauge("cdn_last_success_timestamp", "Last successful reconciliation", ["target"])
TELEGRAM_NOTIFICATIONS = Counter("telegram_notifications_total", "Telegram sends", ["severity", "result"])

