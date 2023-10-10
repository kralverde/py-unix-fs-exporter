import attr

from abc import ABC
from enum import Enum, auto
from math import log2
from typing import Generic, TypeVar, Sequence, Union, Optional, Mapping, Callable

import dag_cbor
import mmh3
from multiformats import CID, multicodec

from hamt_sharding import create_HAMT
from hamt_sharding.buckets import Bucket, BucketPosition

from .content import CONTENT_EXPORTERS
from .ipfs_unix_fs.unix_fs import UnixFS, FSType
from .ipfs_dag_pb.dag_pb import PBNode, PBLink, decode_pbnode

T = TypeVar('T')

class IPFSUnixFSResolveException(Exception): pass

class ExportableType(Enum):
    FILE = auto()
    DIRECTORY = auto()
    OBJECT = auto()
    RAW = auto()
    IDENTITY = auto()

class Exportable(ABC, Generic[T]):
    def __init__(self, 
                 exportable_type: ExportableType,
                 name: str,
                 path: str,
                 cid: CID,
                 depth: int,
                 size: int,
                 node: Union[PBNode, bytes],
                 content: Sequence[T]):
        self.exportable_type = exportable_type
        self.name = name
        self.path = path
        self.cid = cid
        self.depth = depth
        self.size = size
        self.content = content
        self.node = node
    
class FSExportable(Exportable[T], Generic[T]):
    def __init__(self, exportable_type: ExportableType,
                 unix_fs: UnixFS,
                 node: PBNode,
                 name, path, cid, depth, size, content):
        Exportable.__init__(self, exportable_type, name, path, cid, depth, size, node, content)
        self.unix_fs = unix_fs

class UnixFSFile(FSExportable[bytes]):
    def __init__(self,
                 unix_fs, node, name, path, cid, depth, size, content):
        FSExportable.__init__(self, ExportableType.FILE, unix_fs, node, name, path, cid, depth, size, content)

class UnixFSDirectory(FSExportable[Union[Exportable, bytes]]):
    def __init__(self,
                 unix_fs, node, name, path, cid, depth, size, content):
        FSExportable.__init__(self, ExportableType.DIRECTORY, unix_fs, node, name, path, cid, depth, size, content)

class BinaryExportable(FSExportable[T], Generic[T]):
    def __init__(self, exportable_type: ExportableType,
                 node: bytes,
                 name, path, cid, depth, size, content):
        Exportable.__init__(self, exportable_type, name, path, cid, depth, size, node, content)

class ObjectNode(BinaryExportable[object]):
    def __init__(self,
                 node, name, path, cid, depth, size, content):
        BinaryExportable.__init__(self, ExportableType.OBJECT, node, name, path, cid, depth, size, content)

class RawNode(BinaryExportable[bytes]):
    def __init__(self,
                 node, name, path, cid, depth, size, content):
        BinaryExportable.__init__(self, ExportableType.RAW, node, name, path, cid, depth, size, content)

class IdentityNode(BinaryExportable[bytes]):
    def __init__(self,
                 node, name, path, cid, depth, size, content):
        BinaryExportable.__init__(self, ExportableType.IDENTITY, node, name, path, cid, depth, size, content)

@attr.define(slots=True)
class NextResult:
    cid: CID
    name: str
    path: str
    to_resolve: Sequence[str]

@attr.define(slots=True)
class ResolveResult:
    entry: Exportable
    next: Optional[NextResult]
    
Resolver = Callable[[CID, str, str, Sequence[str], int, Mapping[str, bytes]], ResolveResult]

def resolve_raw(cid: CID, name: str, path: str, to_resolve: Sequence[str], depth: int, block_from_encoded_cid: Mapping[str, bytes]) -> ResolveResult:
    if len(to_resolve) > 0:
        raise IPFSUnixFSResolveException(f'no link named {path} found in raw node {cid}')
    block = block_from_encoded_cid[cid.encode()]
    return ResolveResult(
        entry=RawNode(
            node=block,
            name=name,
            path=path,
            cid=cid,
            depth=depth,
            size=len(block),
            content=[block]
        ),
        next=None
    )
        
def resolve_identity(cid: CID, name: str, path: str, to_resolve: Sequence[str], depth: int, block_from_encoded_cid: Mapping[str, bytes]) -> ResolveResult:
    if len(to_resolve) > 0:
        raise IPFSUnixFSResolveException(f'no link named {path} found in identity node {cid}')
    block = block_from_encoded_cid[cid.encode()]
    return ResolveResult(
        entry=IdentityNode(
            node=block,
            name=name,
            path=path,
            cid=cid,
            depth=depth,
            size=len(block),
            content=[block]
        ),
        next=None
    )

def resolve_dag_cbor(cid: CID, name: str, path: str, to_resolve: Sequence[str], depth: int, block_from_encoded_cid: Mapping[str, bytes]) -> ResolveResult:
    block = block_from_encoded_cid[cid.encode()]
    obj = dag_cbor.decode(block)
    sub_obj = obj
    sub_path = path

    while len(to_resolve) > 0:
        prop = to_resolve[0]
        if prop in sub_obj:
            sub_path = f'{sub_path}/{prop}'
            sub_obj_cid = CID.decode(sub_obj[prop]) if prop in sub_obj else None
            if sub_obj_cid is not None:
                return ResolveResult(
                    entry=ObjectNode(
                        block,
                        name,
                        path,
                        cid,
                        depth,
                        len(block),
                        [obj],
                    ),
                    next=NextResult(
                        cid=sub_obj_cid,
                        name=prop,
                        path=sub_path,
                        to_resolve=to_resolve[1:],
                    )
                )
            sub_obj = sub_obj[prop]
        else:
            raise IPFSUnixFSResolveException(f'no property named {prop} in cbor node {cid}')
        
    return ResolveResult(
        entry=ObjectNode(
            block,
            name,
            path,
            cid,
            depth,
            len(block),
            [obj],
        ),
        next=None
    )

@attr.define(slots=True)
class _ShardTraversalContext:
    hamt_depth: int
    root_bucket: Bucket[bool]
    last_bucket: Bucket[bool]

def _hash_fn(buf: bytes):
    return mmh3.hash128(buf)[:8][::-1]

def _pad_length(bucket: Bucket):
    return len(hex(bucket.table_size - 1)[2:])

def _add_links_to_hamt_bucket(links: Sequence[PBLink], bucket: Bucket[bool], root_bucket: Bucket[bool]):
    pad_length = _pad_length(bucket)
    for link in links:
        assert link.name
        if len(link.name) == pad_length:
            pos = int(link.name, 16)
            bucket._put_object_at(pos, Bucket(root_bucket._bits, root_bucket._hash, None))
        else:
            root_bucket[link.name[:2]] = True

def _to_prefix(position: int, pad_length: int) -> str:
    return hex(position)[2:].upper().zfill(pad_length)[:pad_length]

def _to_bucket_path(position: BucketPosition[bool]) -> Sequence[Bucket[bool]]:
    bucket = position.bucket
    path = []
    while bucket._parent is not None:
        path.append(bucket)
        bucket = bucket._parent
    path.append(bucket)
    return path[::-1]

def _find_shard_cid(node: PBNode, name: str, block_from_encoded_cid: Mapping[str, bytes], context: Optional[_ShardTraversalContext] = None) -> Optional[CID]:
    if context is None:
        assert node.data
        unix_fs = UnixFS.unmarshal(node.data)
        assert unix_fs.fs_type == FSType.HAMTSHARD
        assert unix_fs.fanout

        root_bucket: Bucket[bool] = create_HAMT(_hash_fn, log2(unix_fs.fanout))
        context = _ShardTraversalContext(1, root_bucket, root_bucket)

    pad_length = _pad_length(context.last_bucket)
    _add_links_to_hamt_bucket(node.links, context.last_bucket, context.root_bucket)
    position = context.root_bucket._find_new_bucket_and_pos(name)
    prefix = _to_prefix(position.pos, pad_length)
    bucket_path = _to_bucket_path(position)
    if len(bucket_path) > context.hamt_depth:
        context.last_bucket = bucket_path[context.hamt_depth]
        prefix = _to_prefix(context.last_bucket._pos_at_parent, pad_length)

    def predicate(link: PBLink):
        if link.name is None: return False
        entry_prefix = link.name[:pad_length]
        entry_name = link.name[pad_length:]
        if entry_prefix != prefix:
            return False
        if entry_name != '' and entry_name != name:
            return False
        return True

    link = next(filter(predicate, node.links), None)
    if link is None: return None
    if link.name is not None and link.name[pad_length:] == name:
        return link.hash
    
    context.hamt_depth += 1
    block = block_from_encoded_cid[link.hash.encode()]
    decode_pbnode(block)
    return _find_shard_cid(node, name, block_from_encoded_cid, context)

def resolve_dag_pb(cls, cid: CID, name: str, path: str, to_resolve: Sequence[str], depth: int, block_from_encoded_cid: Mapping[str, bytes]) -> ResolveResult:
    block = block_from_encoded_cid[cid.encode()]
    node = decode_pbnode(block)
    assert node.data

    name = name or cid.encode()
    path = path or name

    unix_fs = UnixFS.unmarshal(node.data)
    next_result = None

    if len(to_resolve) > 0:
        if unix_fs.fs_type == FSType.HAMTSHARD:
            link_cid = _find_shard_cid(node, to_resolve[0], block_from_encoded_cid)
        else:
            link_cid = None
            link = next(filter(lambda x: x.name == name, node.links), None)
            if link_cid is not None:
                link_cid = link.hash
        assert link_cid

        next_name = to_resolve[0]
        next_path = f'{path}/{next_name}'

        next_result = NextResult(
            link_cid,
            next_name or '',
            next_path,
            to_resolve[1:],
        )

    content = CONTENT_EXPORTERS[unix_fs.fs_type](cid, node, unix_fs, path, depth, block_from_encoded_cid, resolve)
    assert content is not None
    if unix_fs.is_dir():
        return ResolveResult(
            UnixFSDirectory(
                unix_fs,
                node,
                name,
                path,
                cid,
                depth,
                unix_fs.file_size(),
                content
            ),
            next_result
        )
    return ResolveResult(
        UnixFSFile(
            unix_fs,
            node,
            name,
            path,
            cid,
            depth,
            unix_fs.file_size(),
            content
        ),
        next_result
    )

CONTENT_RESOLVERS = dict[int, Resolver]({
    multicodec.get('dag-pb').code: resolve_dag_pb,
    multicodec.get('raw').code: resolve_raw,
    multicodec.get('dag-cbor').code: resolve_dag_cbor,
    multicodec.get('identity').code: resolve_identity
})

def resolve(cid: CID, name: str, path: str, to_resolve: Sequence[str], depth: int, block_from_encoded_cid: Mapping[str, bytes]) -> ResolveResult:
    resolver = CONTENT_RESOLVERS[cid.codec]
    return resolver(cid, name, path, to_resolve, depth, block_from_encoded_cid)    
