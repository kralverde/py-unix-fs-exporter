import attr

from typing import Sequence

from google.protobuf.message import DecodeError  # type: ignore
from multiformats import CID

from . import dag_pb_pb2 as pb2

class DAGPBFormatException(Exception): pass

@attr.define(slots=True, frozen=True)
class PBLink:
    name: str
    t_size: int
    cid: CID

@attr.define(slots=True, frozen=True)
class PBNode:
    data: bytes
    links: Sequence[PBLink]

    @classmethod
    def decode(cls, raw: bytes) -> 'PBNode':
        raw_node = pb2.PBNode()
        try:
            raw_node.ParseFromString(raw)
        except DecodeError:
            raise DAGPBFormatException()
        return PBNode(
            data=raw_node.Data,
            links=[PBLink(
                name=link.Name,
                t_size=link.Tsize,
                cid=CID.decode(link.Hash)
            ) for link in raw_node.Links]
        )

    def encode(self) -> bytes:
        node = pb2.PBNode()
        if self.data != pb2._PBNODE.fields_by_name['Data'].default_value:
            node.Data = self.data
        if self.links != pb2._PBNODE.fields_by_name['Links'].default_value:
            for link in self.links:
                pb_link = pb2.PBLink()
                if link.name != pb2._PBLINK.fields_by_name['Name'].default_value:
                    pb_link.Name = link.name
                if link.t_size != pb2._PBLINK.fields_by_name['Tsize'].default_value:
                    pb_link.Tsize = link.t_size
                if bytes(link.cid) != pb2._PBLINK.fields_by_name['Hash'].default_value:
                    pb_link.Hash = bytes(link.cid)
                node.Links.append(pb_link)

        return node.SerializeToString() # type: ignore
