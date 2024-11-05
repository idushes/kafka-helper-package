from json import loads
from os import environ
from typing import Type
import re
from dataclasses_avroschema import AvroModel
from httpx import AsyncClient
from .logger import logger


SCHEMA_REGISTRY_URL = environ.get("SCHEMA_REGISTRY_URL")


async def validate_schemas(
        produce_schemas: list[Type[AvroModel]] = None,
        consume_schemas: list[Type[AvroModel]] = None
):
    """ Validate Avro schemas, raises an exception if the schema is not compatible """
    if SCHEMA_REGISTRY_URL is None:
        logger.warning("SCHEMA_REGISTRY_URL environment variable not set, schemas not validated")
        return
    produce_schemas = produce_schemas or []
    consume_schemas = consume_schemas or []
    for model in produce_schemas:
        await validate_avro(model_type=model, schema_owner=True)
    for model in consume_schemas:
        await validate_avro(model_type=model, schema_owner=False)


async def validate_avro(model_type: Type[AvroModel], schema_owner: bool):
    """ Validate Avro schema, raises an exception if the schema is not compatible """
    if SCHEMA_REGISTRY_URL is None:
        logger.warning("SCHEMA_REGISTRY_URL environment variable not set, schema not validated")
        return
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


async def get_schema(schema_id: int) -> dict:
    """ Get schema by id, raises an exception if the schema is not found """
    if SCHEMA_REGISTRY_URL is None:
        raise Exception("SCHEMA_REGISTRY_URL environment variable not set")
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    async with AsyncClient() as client:
        response = await client.get(url=url)
        if response.status_code != 200:
            raise Exception(response.text)
        data = response.json()
        schema_str = data.get("schema")
        return loads(schema_str)


def to_kebab_case(name: str) -> str:
    """ Convert CamelCase to kebab-case, example: CamelCase -> camel-case """
    # Add a hyphen before transitions from lowercase letters or digits to uppercase letters,
    # but ignore consecutive uppercase letters (acronyms).
    kebab_case_name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '-', name)

    # Add a hyphen between consecutive uppercase letters and transitions to lowercase,
    # ensuring acronyms are handled properly.
    kebab_case_name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1-\2', kebab_case_name)

    # Convert the entire string to lowercase.
    return kebab_case_name.lower()
