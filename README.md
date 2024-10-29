# My Kafka-Avro Package

This package provides utilities for working with Kafka and Avro schemas in Python, focusing on Kafka producers, consumers, and schema validation with a schema registry.

## Features

- Kafka producers and consumers integration.
- Avro schema validation against a schema registry.
- Supports asynchronous communication with Kafka and schema registry.

## Installation

You can install the package via pip:

```bash
pip install kafka-avro-helper
```

Usage

Environment Variables

Make sure to set the following environment variables before running the package:

	•	KAFKA_BROKERS: Kafka brokers address
	•	SCHEMA_REGISTRY_URL: URL of the Avro Schema Registry
