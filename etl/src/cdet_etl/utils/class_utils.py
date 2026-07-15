import importlib

def instantiate_class(class_path: str, *args, **kwargs):
    """
    Resolves raw class strings and triggers constructors via reflection.
    """
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        target_cls = getattr(module, class_name)
        return target_cls(*args, **kwargs)
    except Exception as reflection_error:
        raise ImportError(
            f"Failed to dynamically reflect component at path '{class_path}'. "
            f"Detail: {reflection_error}"
        ) from reflection_error
