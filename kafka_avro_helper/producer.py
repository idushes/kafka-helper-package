from os import environ
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from dataclasses_avroschema import AvroModel
import struct


MAGIC_BYTE = 0


__producer__: AIOKafkaProducer = None   # NOQA


def value_serializer(value: Any) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, AvroModel):
        value.validate()
        serialized_data = value.serialize()
        schema_id = value.get_metadata().schema_id  # NOQA
        prefix_bytes = struct.pack(">bI", MAGIC_BYTE, schema_id)
        return prefix_bytes + serialized_data
    raise NotImplementedError(f"Value {value} of type {type(value)} not supported")


def key_serializer(value: Any) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    else:
        return str(value).encode('utf-8')


async def get_producer() -> AIOKafkaProducer:
    """ Get a Kafka producer instance, it will be created if it does not exist and started if it is not ready """
    global __producer__
    if not __producer__:
        __producer__ = AIOKafkaProducer(
            bootstrap_servers=environ.get("KAFKA_BROKERS", "localhost:9092"),
            value_serializer=value_serializer,
            key_serializer=key_serializer
        )
    await __producer__.start()
    return __producer__
