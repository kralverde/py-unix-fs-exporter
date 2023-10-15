import os

from unix_fs_exporter.ipfs_unix_fs.unix_fs import UnixFS, MTime, FSType, DEFAULT_FILE_MODE, DEFAULT_DIRECTORY_MODE, UnixFSFormatException
from unix_fs_exporter.ipfs_unix_fs.unixfs_pb2 import Data as PBData

dir_path = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(dir_path, 'fixtures/unix_fs')

raw = open(os.path.join(fixtures_path, 'raw.unixfs'), 'rb').read()
directory = open(os.path.join(fixtures_path, 'directory.unixfs'), 'rb').read()
file = open(os.path.join(fixtures_path, 'file.txt.unixfs'), 'rb').read()
symlink = open(os.path.join(fixtures_path, 'symlink.txt.unixfs'), 'rb').read()

def test_default_marshaling():
    default_data = PBData()
    unmarshaled_default = UnixFS.unmarshal(default_data.SerializeToString())
    default_fs = UnixFS(mode=0)
    assert default_fs.fs_type == unmarshaled_default.fs_type
    assert default_fs.data == unmarshaled_default.data
    assert default_fs.block_sizes == unmarshaled_default.block_sizes
    assert default_fs.hash_type == unmarshaled_default.hash_type
    assert default_fs.fanout == unmarshaled_default.fanout
    assert default_fs.m_time == unmarshaled_default.m_time
    assert default_fs.mode == unmarshaled_default.mode
    assert default_fs.file_size() == unmarshaled_default.file_size()

    original = UnixFS(fs_type=FSType.FILE)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_raw():
    original = UnixFS(fs_type=FSType.RAW, data=b'bananas')
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_directory():
    original = UnixFS(fs_type=FSType.DIRECTORY)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_hamt_directory():
    original = UnixFS(fs_type=FSType.HAMTSHARD)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_file():
    original = UnixFS(fs_type=FSType.FILE, data=b'batata')
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_file_and_blocksize():
    original = UnixFS(fs_type=FSType.FILE)
    original.add_block_size(256)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def test_file_and_remove_blocksize():
    original = UnixFS(fs_type=FSType.FILE)
    original.add_block_size(256)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()
    unmarshaled.remove_block_size(0)
    assert unmarshaled.block_sizes == []

def test_mode():
    mode = int('0555', 8)
    original = UnixFS(fs_type=FSType.FILE)
    original.mode = mode
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode == mode
    assert original.file_size() == unmarshaled.file_size()

def test_default_mode_files():
    original = UnixFS(fs_type=FSType.FILE)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode == DEFAULT_FILE_MODE
    assert original.file_size() == unmarshaled.file_size()

def test_default_mode_directories():
    original = UnixFS(fs_type=FSType.DIRECTORY)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode == DEFAULT_DIRECTORY_MODE
    assert original.file_size() == unmarshaled.file_size()

def test_default_mode_hamt():
    original = UnixFS(fs_type=FSType.HAMTSHARD)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode == DEFAULT_DIRECTORY_MODE
    assert original.file_size() == unmarshaled.file_size()

def test_mtime():
    time = MTime(100, 100)
    original = UnixFS(m_time=time)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time == time
    assert original.mode == unmarshaled.mode
    assert original.file_size() == unmarshaled.file_size()

def truncate_mode_bits():
    original = UnixFS(mode=0o0100644)
    marshaled = original.marshal()
    unmarshaled = UnixFS.unmarshal(marshaled)
    assert original.fs_type == unmarshaled.fs_type
    assert original.data == unmarshaled.data
    assert original.block_sizes == unmarshaled.block_sizes
    assert original.hash_type == unmarshaled.hash_type
    assert original.fanout == unmarshaled.fanout
    assert original.m_time == unmarshaled.m_time
    assert original.mode == unmarshaled.mode == 0o644
    assert original.file_size() == unmarshaled.file_size()

def test_types():
    for fs_type in FSType:
        original = UnixFS(fs_type=fs_type)
        marshaled = original.marshal()
        unmarshaled = UnixFS.unmarshal(marshaled)
        assert original.fs_type == unmarshaled.fs_type == fs_type
        assert original.data == unmarshaled.data
        assert original.block_sizes == unmarshaled.block_sizes
        assert original.hash_type == unmarshaled.hash_type
        assert original.fanout == unmarshaled.fanout
        assert original.m_time == unmarshaled.m_time
        assert original.mode == unmarshaled.mode
        assert original.file_size() == unmarshaled.file_size()

def test_serial_interop():
    unmarshaled = UnixFS.unmarshal(raw)
    assert unmarshaled.data == b'Hello UnixFS\n'
    assert unmarshaled.fs_type == FSType.FILE
    assert unmarshaled.marshal() == raw

def test_serial_directory():
    unmarshaled = UnixFS.unmarshal(directory)
    assert unmarshaled.data == b''
    assert unmarshaled.fs_type == FSType.DIRECTORY
    assert unmarshaled.marshal() == directory

def test_serial_file():
    unmarshaled = UnixFS.unmarshal(file)
    assert unmarshaled.data == b'Hello UnixFS\n'
    assert unmarshaled.fs_type == FSType.FILE
    assert unmarshaled.marshal() == file

def test_serial_symlink():
    unmarshaled = UnixFS.unmarshal(symlink)
    assert unmarshaled.data == b'file.txt'
    assert unmarshaled.fs_type == FSType.SYMLINK
    #assert unmarshaled.marshal() == symlink

def test_bad():
    try:
        UnixFS.unmarshal(bytes.fromhex('135135135134312231a2'))
    except UnixFSFormatException:
        pass
    else:
        assert False

