from .navigable_config import navigable_extensions

def is_navigable(extension: str, content_type: str = "") -> bool:
    """
    Decides whether the resource should be navigated to find internal links
    by the scout.
    """
    navigable = navigable_extensions.get_all()
    
    extension_lower = extension.lower()
    content_type_lower = content_type.lower()
    
    return (
        extension_lower in navigable or 
        any(t in content_type_lower for t in ["html", "json", "xml"])
    )
