import os

from py_unix_fs_exporter.ipfs_unix_fs.unix_fs import UnixFS, MTime
from py_unix_fs_exporter.ipfs_unix_fs.unixfs_pb2 import Data as PBData

dir_path = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(dir_path, 'fixtures/unix_fs')

raw = open(os.path.join(fixtures_path, 'raw.unixfs'), 'rb').read()
directory = open(os.path.join(fixtures_path, 'directory.unixfs'), 'rb').read()
file = open(os.path.join(fixtures_path, 'file.txt.unixfs'), 'rb').read()
symlink = open(os.path.join(fixtures_path, 'symlink.txt.unixfs'), 'rb').read()

def test_raw():
    pass
