import attr

from enum import Enum
from typing import Optional, List

from . import unixfs_pb2 as pb2

@attr.s(slots=True)
class MTime:
    seconds = attr.ib(type=int)
    nano_seconds = attr.ib(type=Optional[int])

class FSType(Enum):
    RAW = 0
    DIRECTORY = 1
    FILE = 2
    METADATA = 3
    SYMLINK = 4
    HAMTSHARD = 5

    @classmethod
    def from_string(cls, s):
        if s == 'raw':
            return cls.RAW
        if s == 'directory':
            return cls.DIRECTORY
        if s == 'file':
            return cls.FILE
        if s == 'metadata':
            return cls.METADATA
        if s == 'symlink':
            return cls.SYMLINK
        if s == 'hamt-sharded-directory':
            return cls.HAMTSHARD
        raise ValueError()
    
DIR_TYPES = (FSType.DIRECTORY, FSType.HAMTSHARD)
DEFAULT_FILE_MODE = int('0644', base=8)
DEFAULT_DIRECTORY_MODE = int('0755', base=8)

class UnixFS:
    def __init__(self,
                 fs_type: FSType,
                 data: Optional[bytes],
                 block_sizes: List[int],
                 hash_type: Optional[int],
                 fanout: Optional[int],
                 m_time: Optional[MTime],
                 mode: int):
        self.fs_type = fs_type
        self.data = data
        self.block_sizes = block_sizes
        self.hash_type = hash_type    
        self.fanout = fanout
        self.m_time = m_time
        self._mode = mode

    @property
    def mode(self):
        return self._mode
    
    @mode.setter
    def mode(self, m):
        if m is None:
            self._mode = DEFAULT_DIRECTORY_MODE if self.is_dir() else DEFAULT_FILE_MODE
        else:
            self._mode = m & 0xfff

    def is_dir(self) -> bool:
        return self.fs_type in DIR_TYPES
    
    def file_size(self) -> int:
        if self.is_dir(): return 0
        total = sum(s for s in self.block_sizes)
        return len(self.data or b'') + total
    
    def add_block_size(self, size: int):
        self.block_sizes.append(size)

    def remove_block_size(self, index: int):
        self.block_sizes.pop(index)

    @classmethod
    def unmarshal(cls, marshaled: bytes) -> 'UnixFS':
        message = pb2.Data()
        message.ParseFromString(marshaled)
        return UnixFS(
            FSType(message.Type),
            message.Data,
            message.blocksizes,
            message.hashType,
            message.fanout,
            MTime(message.mtime.Seconds or 0, message.mtime.FractionalNanoseconds) if message.mtime else None,
            message.mode
        )

    def marshal(self) -> bytes:
        message = pb2.Data()
        return message.SerializeToString()
    