"""arachnite.transport — pluggable delivery backends for the SignalBus."""

from arachnite.transport.base import BaseTransport
from arachnite.transport.local import LocalTransport

__all__ = ["BaseTransport", "LocalTransport"]

# Optional transports — only exported if their dependencies are installed
try:
    from arachnite.transport.mqtt import MQTTTransport  # noqa: F401
    __all__.append("MQTTTransport")
except ImportError:
    pass

try:
    from arachnite.transport.nats import NATSTransport  # noqa: F401
    __all__.append("NATSTransport")
except ImportError:
    pass

try:
    from arachnite.transport.redis import RedisTransport  # noqa: F401
    __all__.append("RedisTransport")
except ImportError:
    pass
