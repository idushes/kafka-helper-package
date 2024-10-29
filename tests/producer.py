import asyncio
from kafka_avro_helper.producer import get_producer
from kafka_avro_helper.validate import validate_schemas
from tests.user_feedback import UserFeedback


async def send_message(producer):
    key = "test-key"
    value = UserFeedback(user_id="test-user-id", text="test-text", audio=None)
    await producer.send_and_wait("user-feedback", key=key, value=value)


async def main():
    producer = await get_producer()
    await validate_schemas(produce_schemas=[UserFeedback])
    try:
        await send_message(producer)
    finally:
        await producer.flush()
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
