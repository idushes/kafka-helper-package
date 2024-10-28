import asyncio
from tests.user_feedback import UserFeedback
from kafka_avro_helper.validate import validate_schemas


async def send_message(producer):
    key = "test-key"
    value = UserFeedback(user_id="test-user-id", text="test-text", audio=None)
    await producer.send_and_wait("user-feedback", key=key, value=value)


async def main():
    from kafka_avro_helper.producer import producer
    await validate_schemas(produce_schemas=[UserFeedback])
    try:
        await producer.start()
        await send_message(producer)
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
