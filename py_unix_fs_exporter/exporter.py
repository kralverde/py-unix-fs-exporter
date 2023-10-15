from typing import Union, Sequence, Tuple, AsyncIterable

import re

from multiformats import CID

from .content import ExportedContent
from .resolvers import resolve, UnixFSDirectory, ExportableType, BlockStore

class ExporterException(Exception): pass

def _to_path_components(path: str) -> Sequence[str]:
    return filter(bool, re.findall(r'(?:[^\\^/]|\\\/)+', ''.join(path.split())))

def _cid_and_rest(path: Union[str, bytes, CID]) -> Tuple[CID, Sequence[str]]:
    if isinstance(path, bytes):
        return CID.decode(path), []
    if isinstance(path, CID):
        return path, []
    
    try:
        return CID.decode(path), []
    except ValueError:
        pass

    if not isinstance(path, str):
        raise ExporterException('path must be CID, bytes, or str')
    if path[:6] == '/ipfs/':
        path = path[6:]
    output = _to_path_components(path)
    return CID.decode(output[0]), output[1:]

async def _walk_path(path: Union[str, CID], block_store: BlockStore):
    cid, to_resolve = _cid_and_rest(path)
    name = cid.encode()
    entry_path = name
    starting_depth = len(to_resolve)
    while True:
        result = await resolve(cid, name, entry_path, to_resolve, starting_depth, block_store)
        yield result.entry
        if result.next is None:
            return

        to_resolve = result.next.to_resolve
        cid = result.next.cid
        name = result.next.name
        entry_path = result.next.path

async def exporter(path: Union[str, CID], block_store: BlockStore):
    async for result in _walk_path(path, block_store):
        pass
    return result

async def _recurse(node: UnixFSDirectory) -> AsyncIterable[ExportedContent]:
    async for file in node.content:
        yield file
        if isinstance(file, bytes):
            continue
        if file.exportable_type == ExportableType.DIRECTORY:
            async for content in _recurse(file):
                yield content

async def recursive_exporter(path: Union[str, CID], block_store: BlockStore) -> AsyncIterable[ExportedContent]:
    node = await exporter(path, block_store)
    yield node
    if node.exportable_type == ExportableType.DIRECTORY:
        async for content in _recurse(node):
            yield content
