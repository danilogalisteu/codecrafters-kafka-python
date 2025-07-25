import struct

from .types.string import decode_string


def decode_header(recv_message):
    header_format = ">hhi"
    header_size = struct.calcsize(header_format)
    if len(recv_message) < header_size:
        return 0, 0, 0, 0, 0
    api_key, api_version, correlation_id = struct.unpack(
        header_format, recv_message[:header_size]
    )
    recv_message = recv_message[header_size:]

    parsed, client_id = decode_string(recv_message)
    if parsed == 0:
        return 0, 0, 0, 0, 0
    recv_message = recv_message[parsed:]

    if len(recv_message) < 1:
        return 0, 0, 0, 0, 0
    assert recv_message[0] == 0, recv_message.hex(" ")

    return header_size + parsed + 1, api_key, api_version, correlation_id, client_id


def encode_header(correlation_id):
    return struct.pack(">i", correlation_id)
