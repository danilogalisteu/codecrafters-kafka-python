import struct


def decode_varint(recv_message):
    total = 0
    for i, value in enumerate(recv_message):
        total += (value & 0b01111111) << (i * 7)
        if value >> 7 == 0:
            return i + 1, total
    return 0, 0


def encode_varint(total):
    data = b""
    while True:
        value = total & 0b01111111
        total >>= 7
        data += struct.pack("B", value | (1 << 7 if total > 0 else 0))
        if total == 0:
            break
    return data
