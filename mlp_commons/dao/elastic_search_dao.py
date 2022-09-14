from __future__ import annotations

import types
import typing as t
from typing import TypeVar

from elasticsearch import Elasticsearch
from typeguard import typechecked
from typing import Optional
from mlp_commons.dao.generic_dao import Dao
from mlp_commons.models.dao_response import DaoResponse
from mlp_commons.models.identifier import Identifier
from mlp_commons.models.meta_data import MetaData
from mlp_commons.models.search_query import SearchQuery
from mlp_commons.utils.utils import map_optional

D = TypeVar('D', bound=MetaData)
Q = TypeVar('Q', bound=SearchQuery)
I = TypeVar('I', bound=Identifier)
Response = DaoResponse[D]

@typechecked
class ElasticSearchDao(Dao[D, I]):
    """Elasticsearch Implementation for the DAO"""
    elasticsearch_client: Elasticsearch
    index: str
    parser_func: types.FunctionType = None  # Parser function to convert a dictionary to the Entity D

    def __init__(self, db_path: str, index: str, parser_func: types.FunctionType, user: Optional[str] = "",
                 password: Optional[str] = ""):
        self.elasticsearch_client = Elasticsearch(db_path, http_auth=(user, password))
        self.index = index
        if parser_func is None or not isinstance(parser_func, types.FunctionType):
            raise ValueError("Invalid `parser_func` found")
        self.parser_func = parser_func

    def create_indice(self, mapping: dict) -> Response:
        try:
            return DaoResponse.success_response(self.elasticsearch_client.indices.create(index=self.index, body= mapping))
        except Exception as e:
            return DaoResponse.error_response('CREATE_MAPPING', str(e))

    def create(self, data: D, identifier: t.Optional[I] = None) -> Response:
        try:
            id = map_optional(identifier, lambda i: i.get_unique_id())
            res = self.elasticsearch_client.index(index=self.index, document=data.to_dict(), id=id)
            return DaoResponse.success_response_with_message('CREATE', data, res['_id'])
        except Exception as e:
            return DaoResponse.error_response('CREATE', str(e))

    def read(self, identifier: I) -> Response:
        try:
            resp = self.elasticsearch_client.get(index=self.index, id=identifier.get_unique_id())
            result = self.parser_func(resp.body["_source"])
            return DaoResponse.success_response_with_message('READ', result, identifier)
        except Exception as e:
            return DaoResponse.error_response('READ', str(e))

    def update(self, data: D, identifier: I) -> Response:
        try:
            res = self.elasticsearch_client.index(index=self.index, document=data.to_dict(),
                                                  id=identifier.get_unique_id())
            return DaoResponse.success_response_with_message('UPDATE', data, res['_id'])
        except Exception as e:
            return DaoResponse.error_response('UPDATE', str(e))

    def delete(self, identifier: I) -> Response:
        try:
            res = self.elasticsearch_client.delete(index=self.index, id=identifier.get_unique_id())
            return DaoResponse.success_response('DELETE', res['_id'])
        except Exception as e:
            return DaoResponse.error_response('DELETE', str(e))

    def search(self, query: Q) -> Response:
        try:
            res = self.elasticsearch_client.search(index=self.index, body=query.get_query())
            return DaoResponse.success_response_with_message('SEARCH', [self.parser_func(x["_source"]) for x in
                                                                        res.body["hits"]["hits"]], query)
        except Exception as e:
            return DaoResponse.error_response('SEARCH', str(e))

    def aggregation_search(self, query: Q) -> Response:
        try:
            res = self.elasticsearch_client.search(index=self.index, body=query.get_query())
            return DaoResponse.success_response_with_message('SEARCH',  res.body, query)
        except Exception as e:
            return DaoResponse.error_response('SEARCH', str(e))


