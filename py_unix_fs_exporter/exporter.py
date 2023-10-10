from typing import Union, Sequence, Mapping, Tuple

import re

from multiformats import CID

from .resolvers import resolve, UnixFSDirectory, ExportableType

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

    assert isinstance(path, str)
    if path[:6] == '/ipfs/':
        path = path[6:]
    output = _to_path_components(path)
    return CID.decode(output[0]), output[1:]

def _walk_path(path: Union[str, CID], block_from_encoded_cid: Mapping[str, bytes]):
    cid, to_resolve = _cid_and_rest(path)
    name = cid.encode()
    entry_path = name
    starting_depth = len(to_resolve)
    while True:
        result = resolve(cid, name, entry_path, to_resolve, starting_depth, block_from_encoded_cid)
        assert result.entry or result.next
        if result.entry is not None:
            yield result.entry
        if result.next is None:
            return
        
        to_resolve = result.next.to_resolve
        cid = result.next.cid
        name = result.next.name
        entry_path = result.next.path

def exporter(path: Union[str, CID], block_from_encoded_cid: Mapping[str, bytes]):
    *_, result = _walk_path(path, block_from_encoded_cid)
    assert result
    return result

def _recurse(node: UnixFSDirectory):
    for file in node.content:
        yield file
        if isinstance(file, bytes):
            continue
        if file.exportable_type == ExportableType.DIRECTORY:
            yield from _recurse(file)

def recursive_exporter(path: Union[str, CID], block_from_encoded_cid: Mapping[str, bytes]):
    node = exporter(path, block_from_encoded_cid)
    yield node
    if node.exportable_type == ExportableType.DIRECTORY:
        yield from _recurse(node)
