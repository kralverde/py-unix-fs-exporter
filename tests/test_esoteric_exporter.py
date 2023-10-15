from random import randint

from multiformats import CID, multicodec, multihash

from py_unix_fs_exporter.exporter import exporter, ExportableType
from py_unix_fs_exporter.content import ContentExtractionException
from py_unix_fs_exporter.ipfs_dag_pb.dag_pb import PBNode, PBLink
from py_unix_fs_exporter.ipfs_unix_fs.unix_fs import UnixFS, FSType

# For python3.7
def randbytes(l: int) -> bytes:
    return bytes([randint(0, 0xff) for _ in range(l)])

def test_unbalanced_dag():
    block_store = {}

    def store_block(buf: bytes, codec: int) -> CID:
        mh = multihash.get('sha2-256').digest(buf)
        cid = CID('base32', 1, codec, mh)
        assert cid.hashfun.code == multihash.get('sha2-256').code
        block_store[bytes(cid)] = buf
        return cid

    raw_blocks = [
        randbytes(5),
        randbytes(3),
        randbytes(6),
        randbytes(10),
        randbytes(4),
        randbytes(7),
        randbytes(8)
    ]

    leaves = [(store_block(r, multicodec.get('raw').code), r) for r in raw_blocks]

    intermediate_node_1 = PBNode(
        UnixFS(
            fs_type=FSType.FILE,
            block_sizes=[
                len(raw_blocks[2]),
                len(raw_blocks[3])
            ]
        ).marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[2]),
                leaves[2][0]
            ),
            PBLink(
                '',
                len(raw_blocks[3]),
                leaves[3][0]
            )
        ]
    )

    intermediate_node_1_buf = intermediate_node_1.encode()
    intermediate_node_1_cid = store_block(intermediate_node_1_buf, multicodec.get('dag-pb').code)

    intermediate_node_2 = PBNode(
        UnixFS(
            fs_type=FSType.FILE,
            block_sizes=[
                len(raw_blocks[1]),
                len(raw_blocks[2]) + len(raw_blocks[3]),
                len(raw_blocks[4])
            ]
        ).marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[1]),
                leaves[1][0]
            ),
            PBLink(
                '',
                len(intermediate_node_1_buf),
                intermediate_node_1_cid
            ),
            PBLink(
                '',
                len(raw_blocks[4]),
                leaves[4][0]
            )
        ]
    )

    intermediate_node_2_buf = intermediate_node_2.encode()
    intermediate_node_2_cid = store_block(intermediate_node_2_buf, multicodec.get('dag-pb').code)

    unix_fs = UnixFS(
        fs_type=FSType.FILE,
        block_sizes=[
            len(raw_blocks[0]),
            len(raw_blocks[1]) + len(raw_blocks[2]) + len(raw_blocks[3]) + len(raw_blocks[4]),
            len(raw_blocks[5]),
            len(raw_blocks[6])
        ]
    )
    root_node = PBNode(
        unix_fs.marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[0]),
                leaves[0][0]
            ),
            PBLink(
                '',
                len(intermediate_node_2_buf),
                intermediate_node_2_cid
            ),
            PBLink(
                '',
                len(raw_blocks[5]),
                leaves[5][0]
            ),
            PBLink(
                '',
                len(raw_blocks[6]),
                leaves[6][0]
            )
        ]
    )

    root_buf = root_node.encode()
    root_cid = store_block(root_buf, multicodec.get('dag-pb').code)

    exported = exporter(root_cid, block_store)
    assert exported.exportable_type == ExportableType.FILE
    data = b''.join(exported.content)
    assert data == b''.join(raw_blocks)

def test_deep_dag():
    block_store = {}

    def store_block(buf: bytes, codec: int) -> CID:
        mh = multihash.get('sha2-256').digest(buf)
        cid = CID('base32', 1, codec, mh)
        assert cid.hashfun.code == multihash.get('sha2-256').code
        block_store[bytes(cid)] = buf
        return cid

    original_buf = randbytes(5)
    buf = original_buf
    child_cid = store_block(buf, multicodec.get('raw').code)
    for _ in range(10000):
        parent = PBNode(
            UnixFS(
                fs_type=FSType.FILE,
                block_sizes=[
                    len(original_buf)
                ]
            ).marshal(),
            [
                PBLink(
                    '',
                    len(buf),
                    child_cid,
                )
            ]
        )
        buf = parent.encode()
        child_cid = store_block(buf, multicodec.get('dag-pb').code)

    exported = exporter(child_cid, block_store)
    assert exported.exportable_type == ExportableType.FILE
    assert b''.join(exported.content) == original_buf

def test_error_on_too_large_block_sizes():
    block_store = {}

    def store_block(buf: bytes, codec: int) -> CID:
        mh = multihash.get('sha2-256').digest(buf)
        cid = CID('base32', 1, codec, mh)
        assert cid.hashfun.code == multihash.get('sha2-256').code
        block_store[bytes(cid)] = buf
        return cid

    raw_blocks = [
        randbytes(5),
        randbytes(3),
        randbytes(6)
    ]

    leaves = [(store_block(r, multicodec.get('raw').code), r) for r in raw_blocks]

    unix_fs = UnixFS(
        fs_type=FSType.FILE,
        block_sizes=[
            len(raw_blocks[0]),
            len(raw_blocks[1]) + 5,
            len(raw_blocks[2]),
        ]
    )
    root_node = PBNode(
        unix_fs.marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[0]),
                leaves[0][0]
            ),
            PBLink(
                '',
                len(raw_blocks[1]),
                leaves[1][0]
            ),
            PBLink(
                '',
                len(raw_blocks[2]),
                leaves[2][0]
            ),
        ]
    )
    root_buf = root_node.encode()
    root_cid = store_block(root_buf, multicodec.get('dag-pb').code)
    exported = exporter(root_cid, block_store)

    assert exported.exportable_type == ExportableType.FILE
    try:
        [_ for _ in exported.content]
    except ContentExtractionException:
        pass
    else:
        assert False

def test_error_on_too_small_block_sizes():
    block_store = {}

    def store_block(buf: bytes, codec: int) -> CID:
        mh = multihash.get('sha2-256').digest(buf)
        cid = CID('base32', 1, codec, mh)
        assert cid.hashfun.code == multihash.get('sha2-256').code
        block_store[bytes(cid)] = buf
        return cid

    raw_blocks = [
        randbytes(5),
        randbytes(3),
        randbytes(6)
    ]

    leaves = [(store_block(r, multicodec.get('raw').code), r) for r in raw_blocks]

    unix_fs = UnixFS(
        fs_type=FSType.FILE,
        block_sizes=[
            len(raw_blocks[0]),
            len(raw_blocks[1]) - 2,
            len(raw_blocks[2]),
        ]
    )
    root_node = PBNode(
        unix_fs.marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[0]),
                leaves[0][0]
            ),
            PBLink(
                '',
                len(raw_blocks[1]),
                leaves[1][0]
            ),
            PBLink(
                '',
                len(raw_blocks[2]),
                leaves[2][0]
            ),
        ]
    )
    root_buf = root_node.encode()
    root_cid = store_block(root_buf, multicodec.get('dag-pb').code)
    exported = exporter(root_cid, block_store)

    assert exported.exportable_type == ExportableType.FILE
    try:
        [_ for _ in exported.content]
    except ContentExtractionException:
        pass
    else:
        assert False

def test_error_on_wrong_number_block_sizes():
    block_store = {}

    def store_block(buf: bytes, codec: int) -> CID:
        mh = multihash.get('sha2-256').digest(buf)
        cid = CID('base32', 1, codec, mh)
        assert cid.hashfun.code == multihash.get('sha2-256').code
        block_store[bytes(cid)] = buf
        return cid

    raw_blocks = [
        randbytes(5),
        randbytes(3),
        randbytes(6)
    ]

    leaves = [(store_block(r, multicodec.get('raw').code), r) for r in raw_blocks]

    unix_fs = UnixFS(
        fs_type=FSType.FILE,
        block_sizes=[
            len(raw_blocks[0]),
            #len(raw_blocks[1]) + 5,
            len(raw_blocks[2]),
        ]
    )
    root_node = PBNode(
        unix_fs.marshal(),
        [
            PBLink(
                '',
                len(raw_blocks[0]),
                leaves[0][0]
            ),
            PBLink(
                '',
                len(raw_blocks[1]),
                leaves[1][0]
            ),
            PBLink(
                '',
                len(raw_blocks[2]),
                leaves[2][0]
            ),
        ]
    )
    root_buf = root_node.encode()
    root_cid = store_block(root_buf, multicodec.get('dag-pb').code)
    exported = exporter(root_cid, block_store)

    assert exported.exportable_type == ExportableType.FILE
    try:
        [_ for _ in exported.content]
    except ContentExtractionException:
        pass
    else:
        assert False
