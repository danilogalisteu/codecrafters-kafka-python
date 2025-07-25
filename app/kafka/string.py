import struct


def decode_string_compact(recv_message):
    if len(recv_message) < 1:
        return 0, b""
    length = recv_message[0]
    if len(recv_message) < length:
        return 0, b""
    return length, recv_message[1:length].decode()


def decode_string(recv_message):
    if len(recv_message) < 2:
        return 0, b""
    (length,) = struct.unpack(">h", recv_message[:2])
    if len(recv_message) < 2 + length:
        return 0, b""
    return 2 + length, recv_message[2 : 2 + length].decode()
