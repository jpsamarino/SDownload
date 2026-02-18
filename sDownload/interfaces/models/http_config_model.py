from dataclasses import dataclass, field
from .proxy_models import ProxyConfigModel


@dataclass
class HttpConfigModel:
    headers: dict[str, str] = field(default_factory=dict)
    timeout_connect_s: float = 15.0
    valid_ssl: bool = True
    proxy: ProxyConfigModel | None = None
    cookies: dict[str, str] = field(default_factory=dict)
