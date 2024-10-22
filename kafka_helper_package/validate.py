import logging
from os import environ
from typing import Type
import re
from dataclasses_avroschema import AvroModel
from httpx import AsyncClient


SCHEMA_REGISTRY_URL = environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
logger = logging.getLogger("uvicorn")


async def validate_schemas(produce_schemas: list[Type[AvroModel]], consume_schemas: list[Type[AvroModel]]):
    for model in produce_schemas:
        await validate_avro(model_type=model, schema_owner=True)
    for model in consume_schemas:
        await validate_avro(model_type=model, schema_owner=False)


async def validate_avro(model_type: Type[AvroModel], schema_owner: bool):
    schema = model_type.avro_schema()
    topic = to_kebab_case(model_type.__name__)
    subject = topic + "-value"
    logger.info(f"Validating {topic} schema")
    compatibility = "/compatibility" if not schema_owner else ""
    url = f"{SCHEMA_REGISTRY_URL}{compatibility}/subjects/{subject}/versions"
    async with AsyncClient() as client:
        response = await client.post(url=url, json={"schema": schema})
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        if schema_owner is True:
            model_type._metadata.schema_id = data.get("id") # NOQA
        if schema_owner is False and data.get("is_compatible") is False:
            raise Exception(f"{topic} schema is not compatible")
        return data


def to_kebab_case(name: str) -> str:
    # Add a hyphen before transitions from lowercase letters or digits to uppercase letters,
    # but ignore consecutive uppercase letters (acronyms).
    kebab_case_name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '-', name)

    # Add a hyphen between consecutive uppercase letters and transitions to lowercase,
    # ensuring acronyms are handled properly.
    kebab_case_name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1-\2', kebab_case_name)

    # Convert the entire string to lowercase.
    return kebab_case_name.lower()
