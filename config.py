import os
HOST = os.getenv('TCP_HOST', '0.0.0.0')
BASE_PORTS = list(range(300, 380))
ADDITIONAL_PORTS = [6785, 623, 724, 721, 723]
PORTS = sorted(set(BASE_PORTS + ADDITIONAL_PORTS))
COMMAND_PORT = 622
WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.getenv('WEB_PORT', '8888'))
TRUSTED_NETWORKS = [x.strip() for x in os.getenv('TRUSTED_NETWORKS', '127.0.0.1/32').split(',') if x.strip()]
MAX_EVENTS_PER_PORT = 500
READ_CHUNK_SIZE = 65536
LOG_CHUNK_EVENTS = False
LOG_PAYLOADS = False


def parse_positive_int(name, value, default):
    try:
        number = int(value or default)
    except ValueError:
        number = 0
    if number < 1:
        raise ValueError(f'{name} must be a positive integer, got {value!r}')
    return number


def parse_switch(name, value, default):
    value = (value or default).strip().lower()
    if value not in ('on', 'off'):
        raise ValueError(f"{name} must be 'on' or 'off', got {value!r}")
    return value == 'on'


# PakBus logger check: the Pi opens a session the way LoggerNet does (ring, Hello, finished)
# with a client that is alone and silent on its port, addressed to the port's pakbus in
# sites.json. Any valid answer, from whatever node, shows the client is a logger and alive.
PAKBUS_PROBE = parse_switch('PAKBUS_PROBE', os.getenv('PAKBUS_PROBE'), 'on')
PROBE_NODE = parse_positive_int('PROBE_NODE', os.getenv('PROBE_NODE'), 4094)
PROBE_INTERVAL = parse_positive_int('PROBE_INTERVAL', os.getenv('PROBE_INTERVAL'), 300)
if PROBE_NODE > 4094:
    raise ValueError(f'PROBE_NODE must be 1-4094, got {PROBE_NODE}')

# TCP keepalive (off by default: the cellular links seen so far never answer it, so it
# dropped healthy idle loggers). When on, a silent peer is dropped after about
# IDLE + INTERVAL * COUNT seconds.
KEEPALIVE = parse_switch('KEEPALIVE', os.getenv('KEEPALIVE'), 'off')
KEEPALIVE_IDLE = parse_positive_int('KEEPALIVE_IDLE', os.getenv('KEEPALIVE_IDLE'), 60)
KEEPALIVE_INTERVAL = parse_positive_int('KEEPALIVE_INTERVAL', os.getenv('KEEPALIVE_INTERVAL'), 15)
KEEPALIVE_COUNT = parse_positive_int('KEEPALIVE_COUNT', os.getenv('KEEPALIVE_COUNT'), 4)
