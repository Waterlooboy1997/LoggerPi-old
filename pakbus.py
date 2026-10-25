"""Build PakBus packets for the relay's own logger check.

Header layout, quoting and the signature nullifier follow PyCampbellCR1000's pakbus.py.
tests/test_probe.py checks the output byte-for-byte against packets LoggerNet sent in
real captures, so the Pi says exactly what LoggerNet says when it starts a session.
"""
from tap import SYNC, compute_signature

LINK_OFFLINE, LINK_RING, LINK_READY, LINK_FINISHED = 0x8, 0x9, 0xA, 0xB
HELLO_COMMAND, HELLO_RESPONSE = 0x09, 0x89


def signature_nullifier(sig):
    """Two bytes that bring the packet's signature to zero."""
    nullifier = nulb = b''
    for _ in range(2):
        sig = compute_signature(nulb, sig)
        sig2 = (sig << 1) & 0x1FF
        if sig2 >= 0x100:
            sig2 += 1
        nulb = bytes([(0x100 - (sig2 + (sig >> 8))) & 0xFF])
        nullifier += nulb
    return nullifier


def quote(data):
    return data.replace(b'\xBC', b'\xBC\xDC').replace(b'\xBD', b'\xBC\xDD')


def frame(packet):
    """Sign, quote and wrap a packet in sync bytes, ready to write to the socket."""
    signed = packet + signature_nullifier(compute_signature(packet))
    return bytes([SYNC]) + quote(signed) + bytes([SYNC])


def link_header(link, dst, src, expect_more=0, priority=0):
    return ((link << 12) | dst).to_bytes(2, 'big') + ((expect_more << 14) | (priority << 12) | src).to_bytes(2, 'big')


def header(link, dst, src, expect_more=0, priority=0, proto=0, hops=0):
    return link_header(link, dst, src, expect_more, priority) + ((proto << 12) | dst).to_bytes(2, 'big') + ((hops << 12) | src).to_bytes(2, 'big')


def ring(dst, src):
    return frame(link_header(LINK_RING, dst, src))


def hello_command(dst, src, tran, verify_interval=60):
    # IsRouter 1, HopMetric 2, as LoggerNet sends.
    body = bytes([HELLO_COMMAND, tran, 1, 2]) + verify_interval.to_bytes(2, 'big')
    return frame(header(LINK_READY, dst, src, expect_more=1, priority=2) + body)


def finished(dst, src):
    return frame(header(LINK_FINISHED, dst, src, priority=3))
