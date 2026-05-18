from utils.dict_rw import DictWriter
from dataclasses import dataclass, field
from utils.memory_rw import MemoryDictWriter


@dataclass(frozen=True)
class WebCfg:
    web_urls: dict[str, str] = field(default_factory=dict)
    output_writer: DictWriter = MemoryDictWriter()
