"""One PakBus handshake per connection. No reconnect or ping retry loop."""
import time
from pycampbellcr1000 import CR1000
from pycampbellcr1000.pakbus import PakBus

CONNECTION_TIMEOUT_SECONDS = 20
CONNECTION_TIMEOUT_EXIT = 125


class SingleAttemptCR1000(CR1000):
    def __init__(self, link, dest_addr=None, dest=1, src_addr=None, src=2050, security_code=0):
        self.connected=False
        self.pakbus=None
        try:
            link.open()
            self.pakbus=PakBus(link,dest_addr,dest,src_addr,src,security_code)
            packet,transaction=self.pakbus.get_hello_cmd()
            self.pakbus.write(packet)
            until=time.monotonic()+CONNECTION_TIMEOUT_SECONDS
            while time.monotonic()<until:
                response=self.pakbus.wait_packet(transaction)
                if response and response[0] and response[1]:
                    self.connected=True
                    return
                # An unsolicited frame is not a reason to resend or reconnect.
                # Keep waiting for the one hello already sent until the deadline.
            raise TimeoutError('No PakBus response within 20 seconds')
        except Exception:
            link.close()
            raise

    def __del__(self):
        if self.connected and self.pakbus is not None:
            self.bye()
