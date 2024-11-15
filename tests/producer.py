import asyncio
import logging
from kafka_avro_helper.producer import get_producer, send_message
from kafka_avro_helper.validate import validate_schemas
from tests.user_feedback import UserFeedback

logging.basicConfig(level=logging.INFO)


async def main():
    producer = await get_producer()
    await validate_schemas(produce_schemas=[UserFeedback])
    try:
        key = "test-key"
        value = UserFeedback(user_id="test-user-id", text="test-text", audio=None)
        await send_message("user-feedback", key=key, value=value)
    finally:
        await producer.flush()
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
