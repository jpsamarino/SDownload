from dataclasses import dataclass, field

from sDownload.global_settings import global_settings

from .proxy_models import ProxyConfigModel


@dataclass
class HttpConfigModel:
    headers: dict[str, str] = field(default_factory=dict)
    timeout_connect_s: float = field(
        default_factory=lambda: global_settings.default_timeout_connect_s
    )
    valid_ssl: bool = True
    proxy: ProxyConfigModel | None = None
    cookies: dict[str, str] = field(default_factory=dict)
