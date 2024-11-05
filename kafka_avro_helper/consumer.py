import inspect
from asyncio import sleep
from struct import unpack
from io import BytesIO
import fastavro  # NOQA
from dataclasses import dataclass, field
from typing import Callable, Union, get_origin, get_args, Awaitable, Optional, Dict, Any
from os import environ
from aiokafka import AIOKafkaConsumer
from types import UnionType
from dataclasses_avroschema import AvroModel
from .producer import get_producer, AIOKafkaProducer
from .validate import to_kebab_case, get_schema
from .logger import logger


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
    """ Deserialize Avro data to AvroModel """
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


async def get_consumer(topics: Optional[list[str]] = None, postfix: str = "") -> AIOKafkaConsumer:
    """ postfix is used to create a unique group_id """
    KAFKA_BROKERS = environ.get("KAFKA_BROKERS")
    _consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BROKERS or "localhost",
        enable_auto_commit=False,
        group_id=KAFKA_CONSUMER_GROUP_ID + postfix,
        auto_offset_reset='earliest',
    )
    if KAFKA_BROKERS is None:
        logger.warning("KAFKA_BROKERS environment variable not set, consumer not started")
        return _consumer
    await _consumer.start()
    if topics:
        _consumer.subscribe(topics=topics)
        logger.info(f"Consumer started, listening to topics: {topics}")
    else:
        _consumer.subscribe(pattern=".*")
        logger.info("Consumer started, listening to all topics")
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


async def consume_messages(callback: Callback, postfix: str = "") -> (AIOKafkaConsumer, AIOKafkaProducer):
    """ Consume messages from Kafka, process them and send the result to another topic """
    value_annotation = extract_value_annotation(callback)
    topics = list(value_annotation.keys())
    consumer = await get_consumer(topics=topics, postfix=postfix)
    producer = await get_producer()
    if len(consumer.subscription()) == 0:
        return consumer, producer
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
            logger.warning(f"Error decoding message ({record.topic} - {record.key}): {e}")
        except Exception as e:
            logger.error(f"Error processing message ({record.topic} - {record.key}): {e}")
            await consumer.seek_to_committed()
            # TODO: send message to dead letter queue
            await sleep(5)
    return consumer, producer
