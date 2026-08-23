import socket

import pytest

from sDownload.utils.net import find_available_port


def _occupy_port(port: int) -> socket.socket:
    """Bind a real socket to the given port and start listening (occupying it)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("localhost", port))
    s.listen(1)
    return s


def test_returns_free_port_when_others_occupied():
    """
    Occupies 2 out of 3 ports in a range.
    find_available_port must return the only free one.
    """
    start = 19100
    end = 19103  # range of 3 ports: 19100, 19101, 19102
    free_port = 19101

    occupied = []
    try:
        for port in [19100, 19102]:
            occupied.append(_occupy_port(port))

        result = find_available_port(start, end)
        assert result == free_port, f"Expected {free_port}, got {result}"
    finally:
        for s in occupied:
            s.close()


def test_raises_when_all_ports_occupied():
    """
    Occupies ALL 3 ports in the range.
    find_available_port must raise RuntimeError.
    """
    start = 19200
    end = 19203  # range of 3 ports: 19200, 19201, 19202

    occupied = []
    try:
        for port in range(start, end):
            occupied.append(_occupy_port(port))

        with pytest.raises(RuntimeError, match="No available ports found"):
            find_available_port(start, end)
    finally:
        for s in occupied:
            s.close()


def test_returns_port_in_range():
    """Basic sanity: the returned port should be within [start, end)."""
    port = find_available_port(19300, 19400)
    assert 19300 <= port < 19400
