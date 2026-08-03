from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PublicAggreDealsV3ApiItem(_message.Message):
    __slots__ = ("price", "quantity", "tradeType", "time", "tradeId")
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    TRADETYPE_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    TRADEID_FIELD_NUMBER: _ClassVar[int]
    price: str
    quantity: str
    tradeType: int
    time: int
    tradeId: str
    def __init__(self, price: _Optional[str] = ..., quantity: _Optional[str] = ..., tradeType: _Optional[int] = ..., time: _Optional[int] = ..., tradeId: _Optional[str] = ...) -> None: ...

class PublicAggreDealsV3Api(_message.Message):
    __slots__ = ("deals", "eventType")
    DEALS_FIELD_NUMBER: _ClassVar[int]
    EVENTTYPE_FIELD_NUMBER: _ClassVar[int]
    deals: _containers.RepeatedCompositeFieldContainer[PublicAggreDealsV3ApiItem]
    eventType: str
    def __init__(self, deals: _Optional[_Iterable[_Union[PublicAggreDealsV3ApiItem, _Mapping]]] = ..., eventType: _Optional[str] = ...) -> None: ...

class PushDataV3ApiWrapper(_message.Message):
    __slots__ = ("channel", "publicAggreDeals", "symbol", "symbolId", "createTime", "sendTime")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    PUBLICAGGREDEALS_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    SYMBOLID_FIELD_NUMBER: _ClassVar[int]
    CREATETIME_FIELD_NUMBER: _ClassVar[int]
    SENDTIME_FIELD_NUMBER: _ClassVar[int]
    channel: str
    publicAggreDeals: PublicAggreDealsV3Api
    symbol: str
    symbolId: str
    createTime: int
    sendTime: int
    def __init__(self, channel: _Optional[str] = ..., publicAggreDeals: _Optional[_Union[PublicAggreDealsV3Api, _Mapping]] = ..., symbol: _Optional[str] = ..., symbolId: _Optional[str] = ..., createTime: _Optional[int] = ..., sendTime: _Optional[int] = ...) -> None: ...
