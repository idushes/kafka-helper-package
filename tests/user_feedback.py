from dataclasses_avroschema import AvroModel
import dataclasses
import typing


@dataclasses.dataclass
class UserFeedback(AvroModel):
    user_id: str
    text: typing.Optional[str]
    audio: typing.Optional[str] = dataclasses.field(metadata={'doc': 'URL to the audio feedback'})
    archived: bool = False

    class Meta:
        schema_name = "com.lisacorp.kafka.avro.UserFeedback"
