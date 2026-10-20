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
