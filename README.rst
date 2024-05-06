This code has been transpiled from `ipfs-unixfs-exporter <https://github.com/ipfs/js-ipfs-unixfs/tree/master/packages/ipfs-unixfs-exporter@88d73a6f1bb30af84ea90286145ad1b894261520>`_ and has minimal changes. 

Usage
-----

DAG-PB, Raw, DAG-CBOR, and Identity codecs are supported.

```
>>> from unix_fs_exporter import UnixFSFile, UnixFSDirectory, RawNode, ObjectNode, IdentityNode
```

Given a block store and a root cid, decode the underlying data.

```
>>> from unix_fs_exporter import RawNode, export, BlockStore
>>> from multiformats import CID, multicodec, multihash
...
>>> class SomeBlockStore(BlockStore):
...     def __init__(self):
...         self.mapping = dict()
...
...     async def get_block(self, cid):
...         return self.mapping[cid.digest]
...
>>> data = b'some data'
>>> mh = multihash.get('sha2-256').digest(data)
>>> cid = CID('base32', 1, 'raw', mh)
>>> block_store = SomeBlockStore()
>>> block_store.mapping[cid.digest] = data
...
>>> async def test():
...     result = await export(cid, block_store)
...     assert isinstance(result, RawNode)
...     
...     async for chunk in result.content:
...         print(chunk)
...
>>> asyncio.run(test())
b'some data'
```

Recursively iterate through a directory.

```
>>> from unix_fs_exporter import recursive_export, UnixFSFile
>>> # Creation of the blockstore is an exercise left to the reader
>>> async def test():
...     entries = recursive_export('bafybeifpaez32hlrz5tmr7scndxtjgw3auuloyuyxblynqmjw5saapewmu', block_store)
...     async for entry in entries:
...         assert isinstance(entry, UnixFSFile)
...         async for chunk in entry.content:
...             assert isinstance(chunk, bytes)
>>> asyncio.run(test())
```
