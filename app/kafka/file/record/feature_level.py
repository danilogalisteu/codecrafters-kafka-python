import struct

from app.kafka.types.string import decode_string_compact
from app.kafka.types.varint import decode_varint


def decode_record_feature_level(buffer: bytes) -> tuple[int, str, int, int, list[str]]:
    pos_name, name = decode_string_compact(buffer)
    buffer = buffer[pos_name:]
    total_length = pos_name

    (feature_level,) = struct.unpack(">H", buffer[:2])
    buffer = buffer[2:]
    total_length += 2

    pos_fields, fields_count = decode_varint(buffer)
    buffer = buffer[pos_fields:]
    total_length += pos_fields

    fields: list[str] = []
    for _ in range(fields_count):
        pass

    return total_length, name, feature_level, fields_count, fields
