from utils.dict_rw import DictWriter
from dataclasses import dataclass, field

from utils.memory_rw import MemoryDictWriter


@dataclass(frozen=True)
class VkCfg:
    vk_service_token: str
    vk_cutoff_date: str | None = None
    vk_groups: dict[str, str] = field(default_factory=dict)
    output_writer: DictWriter = MemoryDictWriter()
