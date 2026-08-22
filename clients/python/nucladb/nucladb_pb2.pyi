from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DistanceMetric(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISTANCE_METRIC_UNSPECIFIED: _ClassVar[DistanceMetric]
    DISTANCE_METRIC_COSINE: _ClassVar[DistanceMetric]
    DISTANCE_METRIC_L2: _ClassVar[DistanceMetric]
    DISTANCE_METRIC_DOT: _ClassVar[DistanceMetric]
DISTANCE_METRIC_UNSPECIFIED: DistanceMetric
DISTANCE_METRIC_COSINE: DistanceMetric
DISTANCE_METRIC_L2: DistanceMetric
DISTANCE_METRIC_DOT: DistanceMetric

class TenantQuota(_message.Message):
    __slots__ = ("max_vectors", "max_qps")
    MAX_VECTORS_FIELD_NUMBER: _ClassVar[int]
    MAX_QPS_FIELD_NUMBER: _ClassVar[int]
    max_vectors: int
    max_qps: float
    def __init__(self, max_vectors: _Optional[int] = ..., max_qps: _Optional[float] = ...) -> None: ...

class CreateTenantRequest(_message.Message):
    __slots__ = ("tenant_id", "quota")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    QUOTA_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    quota: TenantQuota
    def __init__(self, tenant_id: _Optional[str] = ..., quota: _Optional[_Union[TenantQuota, _Mapping]] = ...) -> None: ...

class CreateTenantResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Vector(_message.Message):
    __slots__ = ("id", "values", "metadata", "tenant_id")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    values: _containers.RepeatedScalarFieldContainer[float]
    metadata: _containers.ScalarMap[str, str]
    tenant_id: str
    def __init__(self, id: _Optional[str] = ..., values: _Optional[_Iterable[float]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class InsertRequest(_message.Message):
    __slots__ = ("vector",)
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    vector: Vector
    def __init__(self, vector: _Optional[_Union[Vector, _Mapping]] = ...) -> None: ...

class InsertResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class BatchUpsertRequest(_message.Message):
    __slots__ = ("vectors",)
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[Vector]
    def __init__(self, vectors: _Optional[_Iterable[_Union[Vector, _Mapping]]] = ...) -> None: ...

class BatchUpsertResponse(_message.Message):
    __slots__ = ("upserted",)
    UPSERTED_FIELD_NUMBER: _ClassVar[int]
    upserted: int
    def __init__(self, upserted: _Optional[int] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("id", "tenant_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    def __init__(self, id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("deleted",)
    DELETED_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    def __init__(self, deleted: _Optional[bool] = ...) -> None: ...

class MetadataFilter(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("query", "top_k", "metric", "ef_search", "filters", "tenant_id")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    EF_SEARCH_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    query: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    metric: DistanceMetric
    ef_search: int
    filters: _containers.RepeatedCompositeFieldContainer[MetadataFilter]
    tenant_id: str
    def __init__(self, query: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ..., metric: _Optional[_Union[DistanceMetric, str]] = ..., ef_search: _Optional[int] = ..., filters: _Optional[_Iterable[_Union[MetadataFilter, _Mapping]]] = ..., tenant_id: _Optional[str] = ...) -> None: ...

class ScoredVector(_message.Message):
    __slots__ = ("id", "score", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("matches",)
    MATCHES_FIELD_NUMBER: _ClassVar[int]
    matches: _containers.RepeatedCompositeFieldContainer[ScoredVector]
    def __init__(self, matches: _Optional[_Iterable[_Union[ScoredVector, _Mapping]]] = ...) -> None: ...
