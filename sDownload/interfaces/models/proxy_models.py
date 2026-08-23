from dataclasses import dataclass
from enum import StrEnum


class EProxyType(StrEnum):
    HTTP = "http"
    HTTPS = "https"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"


@dataclass
class SingleProxyConfigModel:
    protocol: EProxyType
    host: str
    port: int
    username: str | None = None
    password: str | None = None


@dataclass
class ProxyConfigModel:
    http: SingleProxyConfigModel | None = None
    https: SingleProxyConfigModel | None = None
    ftp: SingleProxyConfigModel | None = None
    sftp: SingleProxyConfigModel | None = None
    torrent: SingleProxyConfigModel | None = None
    default: SingleProxyConfigModel | None = None
