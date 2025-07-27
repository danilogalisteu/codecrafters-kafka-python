import logging
import struct

from app.kafka.constants import RecordType
from app.kafka.types.varint import decode_varint

from .feature_level import decode_record_feature_level
from .partition import decode_record_partition
from .topic import decode_record_topic


def decode_record_value(
    buffer: bytes,
) -> tuple[
    int,
    int,
    int,
    int,
    dict[str, str | int | bytes | list[int] | list[bytes]],
    list[str],
]:
    pos_value_length, value_length = decode_varint(buffer, signed=True)
    if pos_value_length == 0:
        return 0, 0, 0, 0, {}
    buffer = buffer[pos_value_length:]
    total_length = pos_value_length

    if len(buffer) < value_length:
        return 0, 0, 0, 0, {}

    frame_version, record_type, record_version = struct.unpack(">BBB", buffer[:3])
    buffer = buffer[3:]
    total_length += 3

    record_data: dict[str, str | int | bytes | list[str]] = {}
    fields: list[str] = []

    if record_type == RecordType.TOPIC:
        pos_topic, record_data = decode_record_topic(buffer)
        buffer = buffer[pos_topic:]
        total_length += pos_topic
    elif record_type == RecordType.PARTITION:
        pos_partition, record_data = decode_record_partition(buffer)
        buffer = buffer[pos_partition:]
        total_length += pos_partition
    elif record_type == RecordType.FEATURE_LEVEL:
        pos_feature_level, record_data = decode_record_feature_level(buffer)
        buffer = buffer[pos_feature_level:]
        total_length += pos_feature_level
    else:
        logging.warning("unhandled record type %d", record_type)
        total_length += value_length - 3
        return (
            total_length,
            frame_version,
            record_type,
            record_version,
            record_data,
            fields,
        )

    record_data = {"type": RecordType(record_type), **record_data}

    pos_fields, fields_count = decode_varint(buffer)
    buffer = buffer[pos_fields:]
    total_length += pos_fields

    for _ in range(fields_count):
        pass

    return (
        total_length,
        frame_version,
        record_type,
        record_version,
        record_data,
        fields,
    )
