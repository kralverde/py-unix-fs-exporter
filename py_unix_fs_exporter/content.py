from typing import Mapping, Union, List, Sequence, Callable, TYPE_CHECKING

from multiformats import CID, multicodec

from .ipfs_dag_pb.dag_pb import PBNode, decode_pbnode
from .ipfs_unix_fs.unix_fs import UnixFS, FSType

if TYPE_CHECKING:
    from .resolvers import Resolver, ResolveResult

def _walk_dag(block_from_encoded_cid: Mapping[str, bytes], node: Union[PBNode, bytes], queue: List[int]):
    if isinstance(node, bytes):
        queue.append(node)
        return
    assert node.data
    file = UnixFS.unmarshal(node.data)
    if file.data:
        queue.append(file.data)
    for link in node.links:
        block = block_from_encoded_cid[link.hash.encode()]
        if link.hash.codec == multicodec.get('dag-pb').code:
            child = decode_pbnode(block)
        elif link.hash.codec == multicodec.get('raw').code:
            child = block
        else:
            raise IndexError()
        _walk_dag(block_from_encoded_cid, child, queue)

def file_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: Resolver) -> Sequence[bytes]:
    file_size = unix_fs.file_size()
    assert file_size

    queue = []
    _walk_dag(block_from_encoded_cid, node, queue)
    return queue

def directory_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: Resolver) -> Sequence[ResolveResult]:
    results = []
    for link in node.links:
        link_name = link.name or ''
        link_path = f'{path}/{link_name}'
        result = resolver(link.hash, link_name, link_path, [], depth + 1, block_from_encoded_cid)
        if result:
            results.append(result)
    return results

def _list_hamt_directory(node: PBNode, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: Resolver) -> Sequence[ResolveResult]:
    assert node.data
    unix_fs = UnixFS.unmarshal(node.data)
    assert unix_fs.fanout
    pad_length = len(hex(unix_fs.fanout - 1)[2:])

    results = []
    for link in node.links:
        name = link.name[pad_length:] if link.name is not None else None
        if name is not None and name != '':
            result = resolver(link.hash, name, f'{path}/{name}', [], depth + 1, block_from_encoded_cid)
            results.append(result)
        else:
            block = block_from_encoded_cid[link.hash.encode()]
            node = decode_pbnode(block)
            results.extend(_list_hamt_directory(node, path, depth, block_from_encoded_cid))
    return results

def hamt_sharded_directory_content(cid: CID, node: PBNode, unix_fs: UnixFS, path: str, depth: int, block_from_encoded_cid: Mapping[str, bytes], resolver: Resolver):
    return _list_hamt_directory(node, path, depth, block_from_encoded_cid, resolver)

def _null(*args):
    return []

ExportedContent = Sequence[Union[bytes, ResolveResult]]
ContentExporter = Callable[[CID, PBNode, UnixFS, str, int, Mapping[str, bytes], Resolver], ExportedContent]
CONTENT_EXPORTERS = dict[FSType, ContentExporter]({
    FSType.RAW: file_content,
    FSType.FILE: file_content,
    FSType.DIRECTORY: directory_content,
    FSType.HAMTSHARD: hamt_sharded_directory_content,
    FSType.METADATA: _null,
    FSType.SYMLINK: _null
})
