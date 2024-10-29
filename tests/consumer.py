import asyncio
from kafka_avro_helper.validate import validate_schemas
from tests.slow_qraphql_operation_detected import SlowQraphqlOperationDetected
from tests.user_feedback import UserFeedback
from kafka_avro_helper.consumer import consume_messages, KafkaMessage


async def process_user_feedback(
        value: UserFeedback | SlowQraphqlOperationDetected,
        topic: str,
        key: str,
        **kwargs
) -> [KafkaMessage]:
    return []


async def main():
    await validate_schemas(consume_schemas=[UserFeedback, SlowQraphqlOperationDetected])
    consumer, producer = await consume_messages(callback=process_user_feedback)
    await consumer.stop()
    await producer.flush()
    await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
