from collections.abc import Mapping
import json


def _safe_attrs_assignment(h5_group, a_mapping):
    a_mapping = _clean_dict(a_mapping)
    for key, value in a_mapping.items():
        # Special-case None, which fails too late to catch below.
        if value is None:
            value = "None"
        # Try storing natively.
        try:
            h5_group.attrs[key] = value
        # Fallback: Save the repr, which in many cases can be used to
        # recreate the object.
        except TypeError:
            h5_group.attrs[key] = json.dumps(value)


def _clean_dict(a_mapping):
    a_mapping = dict(a_mapping)
    for k, v in list(a_mapping.items()):
        # Store dictionaries as JSON strings.
        if isinstance(v, Mapping):
            a_mapping[k] = _clean_dict(a_mapping[k])
            continue
        try:
            json.dumps(v)
        except TypeError:
            a_mapping[k] = str(v)
    return a_mapping
