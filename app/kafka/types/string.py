import struct

from .varint import decode_varint, encode_varint


def decode_string(recv_message: bytes) -> tuple[int, str]:
    if len(recv_message) < 2:
        return 0, ""
    (length,) = struct.unpack(">h", recv_message[:2])
    if len(recv_message) < 2 + length:
        return 0, ""
    return 2 + length, recv_message[2 : 2 + length].decode()


def decode_string_compact(recv_message: bytes) -> tuple[int, str]:
    pos, length = decode_varint(recv_message)
    if pos == 0:
        return 0, ""
    if len(recv_message[pos:]) < length - 1:
        return 0, ""
    return length, recv_message[pos : pos + length - 1].decode()


def encode_string(value: str) -> bytes:
    buffer = value.encode()
    return struct.pack(">i", len(buffer)) + buffer


def encode_string_compact(value: str) -> bytes:
    buffer = value.encode()
    return encode_varint(len(buffer) + 1) + buffer
