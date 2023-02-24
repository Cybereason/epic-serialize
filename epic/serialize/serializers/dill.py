"""
Non-default Serializer for dill.
This is NOT a dependency - install dill manually as needed.
"""
from epic.common.importguard import ImportGuard

with ImportGuard("pip install dill"):
    import dill

from .base import DumpLoadSerializer


class DillSerializer(DumpLoadSerializer, registered_name="dill"):
    MODULE = dill
