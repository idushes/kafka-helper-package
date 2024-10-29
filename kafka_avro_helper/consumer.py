import inspect
from struct import unpack
from io import BytesIO
import fastavro  # NOQA
from dataclasses import dataclass, field
from typing import Callable, Union, get_origin, get_args, Awaitable, Optional, Dict, Any
from os import environ
from aiokafka import AIOKafkaConsumer
from types import UnionType
from dataclasses_avroschema import AvroModel
from .producer import get_producer
from .validate import to_kebab_case, get_schema
import logging


KAFKA_BROKERS = environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_CONSUMER_GROUP_ID = environ.get("KAFKA_CONSUMER_GROUP_ID", "default")
schemas = {}


@dataclass
class KafkaMessage:
    topic: str
    key: Any
    value: AvroModel
    headers: Dict[str, str] = field(default_factory=dict)


Callback = Callable[..., Awaitable[list[KafkaMessage]]]


class MagicByteError(ValueError):
    pass


async def value_deserializer(data: Optional[bytes], annotation: AvroModel) -> Optional[AvroModel]:
    global schemas
    if data is None:
        return None
    magic_byte = data[0]
    if magic_byte != 0:
        raise MagicByteError("Invalid magic byte, expected 0.")
    schema_id = unpack('>I', data[1:5])[0]
    schema = schemas.get(schema_id)
    if schema is None:
        schema = await get_schema(schema_id=schema_id)
        schemas[schema_id] = schema
    avro_data = BytesIO(data[5:])
    decoded_message = fastavro.schemaless_reader(avro_data, schema)
    value = annotation.parse_obj(decoded_message)
    value.validate()
    return value


async def get_consumer(topics: Optional[list[str]] = None) -> AIOKafkaConsumer:
    _consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS,
        enable_auto_commit=False,
        group_id=KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
    )
    await _consumer.start()
    if topics:
        _consumer.subscribe(topics=topics)
    else:
        _consumer.subscribe(pattern=".*")
    return _consumer


def extract_value_annotation(callback: Callback) -> Dict[str, AvroModel]:
    """ Extract topic annotation from callback, example: {'topic1': AvroModel1, 'topic2': AvroModel2} """
    value_annotation = {}

    def pick_event(_annotation):
        if issubclass(_annotation, AvroModel):
            topic = to_kebab_case(_annotation.__name__)
            if topic in value_annotation.keys():
                raise Exception(f"Duplicate topic {topic}")
            value_annotation[topic] = _annotation

    signature = inspect.signature(callback)
    for param_name, param in signature.parameters.items():
        if param.name == 'value':
            if get_origin(param.annotation) in [UnionType, Union]:
                for annotation in get_args(param.annotation):
                    pick_event(annotation)
            else:
                pick_event(param.annotation)
    return value_annotation


async def consume_messages(callback: Callback):
    value_annotation = extract_value_annotation(callback)
    topics = list(value_annotation.keys())
    consumer = await get_consumer(topics)
    producer = await get_producer()
    async for record in consumer:
        try:
            ValueAnotation = value_annotation[record.topic]
            key = record.key.decode('utf-8') if record.key else None
            value = await value_deserializer(data=record.value, annotation=ValueAnotation)
            messages = await callback(
                value=value,
                key=key,
                headers={k: v.decode('utf-8') for k, v in record.headers},
                topic=record.topic,
            )
            for message in messages:
                if not issubclass(type(message), KafkaMessage):
                    raise Exception(f"Event {message} is not a subclass of KafkaEventBase")
                await producer.send_and_wait(
                    topic=message.topic,
                    key=message.key,
                    value=message.value,
                    headers={"processed_topic": record.topic, **message.headers}
                )
            await consumer.commit()
        except (UnicodeDecodeError, MagicByteError) as e:
            logging.warning(f"Error decoding message ({record.topic} - {record.key}): {e}")
        except Exception as e:
            await consumer.seek_to_committed()
            # TODO: send message to dead letter queue
            raise e
