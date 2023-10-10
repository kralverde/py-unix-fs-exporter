import attr

from typing import Optional, Sequence

from multiformats import CID

from . import dag_pb_pb2 as pb2

class DAGPBFormatException(Exception): pass

@attr.s(slots=True, frozen=True)
class PBLink:
    name = attr.ib(type=Optional[str])
    t_size = attr.ib(type=Optional[int])
    hash = attr.ib(type=CID)

@attr.s(slots=True, frozen=True)
class PBNode:
    data = attr.ib(type=Optional[bytes])
    links = attr.ib(type=Sequence[PBLink])

def decode_pbnode(raw: bytes) -> PBNode:
    raw_node = pb2.PBNode()
    raw_node.ParseFromString(raw)

    return PBNode(
        data=raw_node.Data,
        links=[PBLink(
            name=link.Name,
            t_size=link.Tsize,
            hash=CID.decode(link.Hash)
        ) for link in raw_node.Links]
    )
