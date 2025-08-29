import yaml
from types import SimpleNamespace as NS

def _ns(d):
    if isinstance(d, dict):
        return NS(**{k: _ns(v) for k, v in d.items()})
    if isinstance(d, list):
        return [_ns(x) for x in d]
    return d

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return _ns(yaml.safe_load(f))
