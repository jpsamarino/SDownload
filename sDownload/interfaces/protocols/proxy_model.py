from dataclasses import dataclass
from enum import Enum
from typing import Optional


class EProxyProtocol(str, Enum):
    HTTP = "http"
    HTTPS = "https"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"


@dataclass
class SingleProxyConfigModel:
    protocol: EProxyProtocol
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class ProxyConfigModel:
    http: SingleProxyConfigModel | None = None
    https: SingleProxyConfigModel | None = None
    ftp: SingleProxyConfigModel | None = None
    sftp: SingleProxyConfigModel | None = None
    torrent: SingleProxyConfigModel | None = None
    default: SingleProxyConfigModel | None = None
