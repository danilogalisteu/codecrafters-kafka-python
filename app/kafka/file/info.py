import logging
import struct


def decode_info(buffer: bytes) -> tuple[int, int, int, int, int, int, int, int, int]:
    logging.info("pre info %s", buffer[:45].hex(" "))
    info_format = ">HIQQqhiI"
    info_length = struct.calcsize(info_format)
    if len(buffer) < info_length:
        return 0, 0, 0, 0, 0, 0, 0, 0, 0
    return info_length, *struct.unpack(info_format, buffer[:info_length])
