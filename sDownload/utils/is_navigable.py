from .navigable_config import navigable_content_types, navigable_extensions


def is_navigable(extension: str, content_type: str = "") -> bool | None:
    """
    Decide whether a resource should be navigated to find internal links.

    Returns:
        - True  -> should navigate
        - False -> should not navigate
        - None  -> unknown (no extension or content_type)
    """
    ext = extension.lower().strip(".")
    ct = content_type.lower()

    if not ext and not ct:
        return None  # unknown
    if not ext:
        return ct in navigable_content_types
    if ext in navigable_extensions:
        return True
    return ct in navigable_content_types
