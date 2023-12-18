import sys

if sys.version < (3, 11):
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

__version__ = "0.0.1"
