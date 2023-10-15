from typing import Mapping, Union, List, Sequence, Callable, TYPE_CHECKING, Iterator

from multiformats import CID, multicodec

from .ipfs_dag_pb.dag_pb import PBNode, PBLink
from .ipfs_unix_fs.unix_fs import UnixFS, FSType

if TYPE_CHECKING:
    from .resolvers import Resolver, Exportable

class ContentExtractionException(Exception): pass

def _walk_dag(block_from_encoded_cid: Mapping[str, bytes], node: PBNode) -> Iterator[bytes]:
    file = UnixFS.unmarshal(node.data)
    if len(file.block_sizes) != len(node.links):
        raise ContentExtractionException('inconsistent block sizes and DAG links')
    yield file.data
    queue: List[Sequence[PBLink]] = [node.links]
    while True:
        try:
            links = queue.pop()
        except IndexError:
            return
        for i, link in enumerate(links):
            block = block_from_encoded_cid[bytes(link.cid)]
            if link.cid.codec.code == multicodec.get('dag-pb').code:
                node = PBNode.decode(block)
                file = UnixFS.unmarshal(node.data)
                if len(file.block_sizes) != len(node.links):
                    raise ContentExtractionException('inconsistent block sizes and DAG links')
                yield file.data
                defered_links = links[i + 1:]
                queue.append(defered_links)
                queue.append(node.links)
                break
            elif link.cid.codec.code == multicodec.get('raw').code:
                yield block
            else:
                raise ContentExtractionException(f'unsupported codec: {link.cid.codec.code}')

def file_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: 'Resolver') -> Iterator[bytes]:
    assert unix_fs.fs_type == FSType.FILE
    expected_size = unix_fs.file_size()
    read_length = 0
    for chunk in _walk_dag(block_from_encoded_cid, node):
        read_length += len(chunk)
        yield chunk
    if read_length != expected_size:
        raise ContentExtractionException(f'expected to read {expected_size} but read {read_length}')

def raw_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: 'Resolver') -> Iterator[bytes]:
    assert unix_fs.fs_type == FSType.RAW
    return unix_fs.data

def directory_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: 'Resolver') -> Iterator['Exportable']:
    assert unix_fs.fs_type == FSType.DIRECTORY
    for link in node.links:
        link_path = f'{path}/{link.name}'
        result = resolver(link.cid, link.name, link_path, [], depth + 1, block_from_encoded_cid)
        yield result.entry

def _list_hamt_directory(node: PBNode, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: 'Resolver') -> Iterator['Exportable']:
    unix_fs = UnixFS.unmarshal(node.data)
    if unix_fs.fanout == 0:
        raise ContentExtractionException('no fanout for hamt directory')
    pad_length = len(hex(unix_fs.fanout - 1)[2:])
    for link in node.links:
        name = link.name[pad_length:] if link.name is not None else None
        if name is not None and name != '':
            result = resolver(link.cid, name, f'{path}/{name}', [], depth + 1, block_from_encoded_cid)
            yield result.entry
        else:
            block = block_from_encoded_cid[bytes(link.cid)]
            node = PBNode.decode(block)
            yield from _list_hamt_directory(node, path, depth, block_from_encoded_cid)

def hamt_sharded_directory_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: 'Resolver'):
    assert unix_fs.fs_type == FSType.HAMTSHARD
    return _list_hamt_directory(node, path, depth, block_from_encoded_cid, resolver)

def _null(*args):
    return []

ExportedContent = Union[bytes, 'Exportable']
ContentExporter = Callable[[CID, PBNode, UnixFS, str, int, Mapping[str, bytes], 'Resolver'], Iterator[ExportedContent]]
CONTENT_EXPORTERS: Mapping[FSType, ContentExporter] = {
    FSType.RAW: raw_content,
    FSType.FILE: file_content,
    FSType.DIRECTORY: directory_content,
    FSType.HAMTSHARD: hamt_sharded_directory_content,
    FSType.METADATA: _null,
    FSType.SYMLINK: _null
}
