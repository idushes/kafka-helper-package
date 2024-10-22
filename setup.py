from setuptools import setup, find_packages

setup(
    name='kafka-avro-helper',
    version='0.0.1',
    description='A package for Kafka and AVRO processing',
    author='Andrei Boiko',
    author_email='dushes.nadym@gmail.com',
    url='https://github.com/idushes/kafka-helper-package',
    packages=find_packages(),
    install_requires=[
        'aiokafka',
        'httpx',
        'dataclasses-avroschema',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)