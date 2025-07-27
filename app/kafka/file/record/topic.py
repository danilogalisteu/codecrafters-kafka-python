from app.kafka.types.string import decode_string_compact


def decode_record_topic(
    buffer: bytes,
) -> tuple[int, dict[str, str | bytes]]:
    pos_name, name = decode_string_compact(buffer)
    buffer = buffer[pos_name:]
    total_length = pos_name

    topic_uuid = buffer[:16]
    buffer = buffer[16:]
    total_length += 16

    return total_length, {
        "name": name,
        "topic_uuid": topic_uuid,
    }
