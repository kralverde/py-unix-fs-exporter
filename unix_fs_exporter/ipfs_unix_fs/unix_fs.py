import attr

from enum import Enum
from typing import Optional, List

from . import unixfs_pb2 as pb2
from google.protobuf.message import DecodeError  # type: ignore

class UnixFSFormatException(Exception): pass

@attr.define(slots=True, frozen=True)
class MTime:
    seconds: int
    nano_seconds: int

class FSType(Enum):
    RAW = 0
    DIRECTORY = 1
    FILE = 2
    METADATA = 3
    SYMLINK = 4
    HAMTSHARD = 5

    @classmethod
    def from_string(cls, s: str) -> 'FSType':
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
    def __init__(self, *,
                 fs_type: FSType = FSType.RAW,
                 data: Optional[bytes] = None,
                 block_sizes: Optional[List[int]] = None, 
                 hash_type: int = 0,
                 fanout: int = 0,
                 m_time: MTime = MTime(0, 0),
                 mode: Optional[int] = None):
        self.fs_type = fs_type
        self.data = b'' if data is None else data
        self.block_sizes = [] if block_sizes is None else block_sizes
        self.hash_type = hash_type
        self.fanout = fanout
        self.m_time = m_time
        self._original_mode = None 
        self.mode = mode  # type: ignore

    @property
    def mode(self) -> int:
        return self._mode
    
    @mode.setter
    def mode(self, m: Optional[int]) -> None:
        if m is None:
            self._mode = DEFAULT_DIRECTORY_MODE if self.is_dir() else DEFAULT_FILE_MODE
        else:
            self._mode = m & 0xfff

    def __repr__(self) -> str:
        return f'UnixFS({self.fs_type}, {self.data!r}, {self.block_sizes}, {self.hash_type}, {self.fanout}, {self.m_time}, {self.mode} ({self._original_mode}))'

    def is_dir(self) -> bool:
        return self.fs_type in DIR_TYPES
    
    def file_size(self) -> int:
        if self.is_dir(): return 0
        total = sum(s for s in self.block_sizes)
        return len(self.data) + total
    
    def add_block_size(self, size: int) -> None:
        self.block_sizes.append(size)

    def remove_block_size(self, index: int) -> None:
        self.block_sizes.pop(index)

    @classmethod
    def unmarshal(cls, marshaled: bytes) -> 'UnixFS':
        message = pb2.Data()
        try:
            message.ParseFromString(marshaled)
        except DecodeError:
            raise UnixFSFormatException()
        fs = UnixFS(
            fs_type=FSType(message.Type),
            data=message.Data,
            block_sizes=message.blocksizes,
            hash_type=message.hashType,
            fanout=message.fanout,
            m_time=MTime(message.mtime.Seconds, message.mtime.FractionalNanoseconds),
            mode=message.mode
        )
        fs._original_mode = message.mode
        return fs

    def marshal(self) -> bytes:
        message = pb2.Data()

        if self.fs_type.value != pb2._DATA.fields_by_name['Type'].default_value:
            message.Type = self.fs_type.value
        if self.data != pb2._DATA.fields_by_name['Data'].default_value:
            message.Data = self.data
        if self.block_sizes != pb2._DATA.fields_by_name['blocksizes'].default_value:
            message.blocksizes.extend(self.block_sizes)
        if self.hash_type != pb2._DATA.fields_by_name['hashType'].default_value:
            message.hashType = self.hash_type
        if self.fanout != pb2._DATA.fields_by_name['fanout'].default_value:
            message.fanout = self.fanout
        
        if self.m_time.seconds != pb2._UNIXTIME.fields_by_name['Seconds'].default_value:
            message.mtime.Seconds = self.m_time.seconds
        if self.m_time.nano_seconds != pb2._UNIXTIME.fields_by_name['FractionalNanoseconds'].default_value:
            message.mtime.FractionalNanoseconds = self.m_time.nano_seconds

        mode = self.mode
        if self._original_mode:
            mode = self._original_mode
        if self._original_mode or mode != pb2._DATA.fields_by_name['mode'].default_value:
            message.mode = mode

        if self.file_size() != pb2._DATA.fields_by_name['filesize'].default_value:
            message.filesize = self.file_size()

        return message.SerializeToString()  # type: ignore
