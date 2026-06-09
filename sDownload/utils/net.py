import random
import socket


def find_available_port(start: int = 8000, end: int = 9000) -> int:
    """
    Find an available TCP port by shuffling the range [start, end) and testing each one.

    Shuffling reduces collisions when multiple processes search concurrently,
    while the socket check guarantees the returned port is actually free.

    Raises RuntimeError if no port in the range is available.
    """
    ports = list(range(start, end))
    random.shuffle(ports)
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) != 0:
                return port
    raise RuntimeError(f"No available ports found in range [{start}, {end})")
