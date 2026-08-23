from sDownload.utils.is_navigable import is_navigable
from sDownload.utils.navigable_config import navigable_extensions


def test_is_navigable_defaults():
    # Test by extension (Default Blacklist)
    assert is_navigable("html") is True
    assert is_navigable("php") is True
    assert is_navigable("js") is True
    assert is_navigable("zip") is False
    assert is_navigable("pdf") is False

    # Test by content-type
    assert is_navigable("", "text/html") is True
    assert is_navigable("", "application/json") is True
    assert is_navigable("", "image/png") is False


def test_is_navigable_dynamic_configuration():
    # Custom extension: "besdep"
    # By default, it should be False (plain file)
    assert is_navigable("besdep") is False

    # Dynamically add to the registry
    navigable_extensions.add("besdep")

    # Now the crawler should consider it navigable
    assert is_navigable("besdep") is True


def test_is_navigable_combinations():
    assert is_navigable("zip", "text/html") is True
    assert is_navigable("html", "application/octet-stream") is True
    assert is_navigable("rar", "application/x-rar-compressed") is False
