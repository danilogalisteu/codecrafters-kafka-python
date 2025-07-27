from app.kafka.types.string import decode_string_compact
from app.kafka.types.varint import decode_varint


def decode_record_topic(
    buffer: bytes,
) -> tuple[int, dict[str, str | int | bytes | list[str]]]:
    pos_name, name = decode_string_compact(buffer)
    buffer = buffer[pos_name:]
    total_length = pos_name

    topic_uuid = buffer[:16]
    buffer = buffer[16:]
    total_length += 16

    pos_fields, fields_count = decode_varint(buffer)
    buffer = buffer[pos_fields:]
    total_length += pos_fields

    fields: list[str] = []
    for _ in range(fields_count):
        pass

    return total_length, {
        "topic_name": name,
        "topic_uuid": topic_uuid,
        "fields": fields,
    }
