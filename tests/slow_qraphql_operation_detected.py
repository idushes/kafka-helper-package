from dataclasses_avroschema import AvroModel
from dataclasses_avroschema import types
import dataclasses


@dataclasses.dataclass
class SlowQraphqlOperationDetected(AvroModel):
    """
    User generated an new API token
    """
    duration: types.Float32 = dataclasses.field(metadata={'description': 'seconds'})
    query: str

    class Meta:
        schema_name = "com.lisacorp.kafka.avro.SlowQraphqlOperationDetected"
