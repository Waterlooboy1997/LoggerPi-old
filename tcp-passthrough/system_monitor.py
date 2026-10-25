import time
import psutil

class SystemMonitor:
    def __init__(self):
        self.last = 0
        self.cache = {}
        psutil.cpu_percent(interval=None)

    def snapshot(self):
        if time.monotonic() - self.last < 2:
            return self.cache
        temperature = None
        try:
            with open('/sys/class/thermal/thermal_zone0/temp') as f:
                temperature = round(int(f.read().strip()) / 1000, 1)
        except (OSError, ValueError):
            pass
        memory = psutil.virtual_memory()
        self.cache = {'cpu_percent': psutil.cpu_percent(interval=None), 'ram_percent': memory.percent,
                      'ram_used': memory.used, 'ram_total': memory.total, 'cpu_temp_c': temperature,
                      'uptime_seconds': int(time.time() - psutil.boot_time())}
        self.last = time.monotonic()
        return self.cache
