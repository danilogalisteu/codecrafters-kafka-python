import struct

from app.kafka.types.varint import decode_varint


def decode_record_partition(
    buffer: bytes,
) -> tuple[int, dict[str, int | bytes | list[int] | list[bytes]]]:
    partition_id, topic_uuid = struct.unpack(">I16s", buffer[:20])
    buffer = buffer[20:]
    total_length = 20

    pos_replica, replica_count = decode_varint(buffer)
    buffer = buffer[pos_replica:]
    total_length += pos_replica

    replicas: list[int] = []
    for _ in range(replica_count - 1):
        replicas.append(struct.unpack(">I", buffer[:4])[0])
        buffer = buffer[4:]
        total_length += 4

    pos_insync, insync_count = decode_varint(buffer)
    buffer = buffer[pos_insync:]
    total_length += pos_insync

    insync: list[int] = []
    for _ in range(insync_count - 1):
        insync.append(struct.unpack(">I", buffer[:4])[0])
        buffer = buffer[4:]
        total_length += 4

    pos_removing, removing_count = decode_varint(buffer)
    buffer = buffer[pos_removing:]
    total_length += pos_removing

    removing: list[int] = []
    for _ in range(removing_count - 1):
        removing.append(struct.unpack(">I", buffer[:4])[0])
        buffer = buffer[4:]
        total_length += 4

    pos_adding, adding_count = decode_varint(buffer)
    buffer = buffer[pos_adding:]
    total_length += pos_adding

    adding: list[int] = []
    for _ in range(adding_count - 1):
        removing.append(struct.unpack(">I", buffer[:4])[0])
        buffer = buffer[4:]
        total_length += 4

    leader, leader_epoch, partition_epoch = struct.unpack(">III", buffer[:12])
    buffer = buffer[12:]
    total_length += 12

    pos_directories, directories_count = decode_varint(buffer)
    buffer = buffer[pos_directories:]
    total_length += pos_directories

    directories: list[bytes] = []
    for _ in range(directories_count - 1):
        directories.append(buffer[:16])
        buffer = buffer[16:]
        total_length += 16

    return total_length, {
        "partition": partition_id,
        "topic_uuid": topic_uuid,
        "leader": leader,
        "leader_epoch": leader_epoch,
        "partition_epoch": partition_epoch,
        "replicas": replicas,
        "insync": insync,
        "removing": removing,
        "adding": adding,
        "directories": directories,
    }
