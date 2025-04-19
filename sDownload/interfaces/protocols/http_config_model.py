from dataclasses import dataclass, field
from .proxy_model import ProxyConfigModel


@dataclass
class HttpConfigModel:
    headers: dict[str, str] = field(default_factory=dict)
    timeout_connect: int = 15
    valid_ssl: bool = True
    proxy: ProxyConfigModel | None = None
    cookies: dict[str, str] = field(default_factory=dict)
