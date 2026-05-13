from dataclasses import dataclass
import json


@dataclass(frozen=True)
class OMEntity:
    """Minimal OpenMetadata lineage entity compatible with Airflow serialization."""

    entity: type
    fqn: str
    key: str = "default"

    def __str__(self) -> str:
        return json.dumps({
            "entity": f"{self.entity.__module__}.{self.entity.__name__}",
            "fqn": self.fqn,
            "key": self.key,
        })

    def serialize(self) -> str:
        return str(self)
