import logging
import struct


def decode_header(buffer: bytes) -> tuple[int, int, int, int, int, int]:
    logging.debug("pre header %s", buffer[:25].hex(" "))
    header_format = ">QIIBI"
    header_length = struct.calcsize(header_format)
    if len(buffer) < header_length:
        return 0, 0, 0, 0, 0, 0
    return header_length, *struct.unpack(header_format, buffer[:header_length])
