from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, Union, List

from mlp_commons.models.search_query import SearchQuery
from typeguard import typechecked
from typing_extensions import Literal
from fct_python_sdk.entities.elastic_search_queries import elasticsearch_mapping


@dataclass
@typechecked
class ElasticSearchQuery(SearchQuery):
    query: dict

    def get_query(self) -> any:
        return json.loads(json.dumps(self.query))


@typechecked
@dataclass
class ESSuperQueryForFeatureSearch(SearchQuery):
    def get_query(feature_group_search_string: str, tags: List[str], users: List[str], limit: int, offset: int,
                  sort_by: str, order: Literal["asc", "desc"]):
        name = "* " + feature_group_search_string + " *"

        tag_filter = "["
        user_filter = "["

        for tag in tags:
            tag_filter = tag_filter + tag + ","
        tag_filter = tag_filter + "]"

        for user in users:
            user_filter = user_filter + user + ","
        user_filter = user_filter + "]"

        filters = [{
            "bool": {
                "should": [
                    {
                        "query_string": {
                            "fields": [
                                "name"
                            ],
                            "query": name
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }]

        if len(users) != 0:
            filters.append({
                "bool": {
                    "should": [
                        {
                            "match": {
                                "user": user_filter
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            })

        if len(tags) != 0:
            filters.append({
                        "bool": {
                            "should": [
                                {
                                    "match": {
                                        "tags": tag_filter
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    })

        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "bool": {
                                "filter": filters
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "sort": [
                {
                    sort_by: {
                        "order": order
                    }
                }
            ],
            "from": offset, "size": limit
        }
        return query


@typechecked
@dataclass
class ESQueryForFeatureSearch(SearchQuery):
    def get_query(name: str):
        return {
            "query": {
                "bool": {
                    "must": {
                        "term": {"name": name}
                    }
                }
            },
            "sort": [
                {
                    "version": {
                        "order": "desc"
                    }
                }
            ]
        }


@typechecked
@dataclass
class ESQueryForFuzzySearch(SearchQuery):
    def get_query(key: str, val: str, limit: Union[int, None], offset: Union[int, None]):
        val = ".*" + val + ".*"

        if limit is None or offset is None:
            return {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "regexp": {
                                    key: val
                                }
                            }
                        ]
                    }
                }
            }
        else:
            return {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "regexp": {
                                    key: val
                                }
                            }
                        ]
                    }
                },
                "from": offset, "size": limit
            }


class CreateIndexMapping(SearchQuery):
    def get_query():
        return elasticsearch_mapping


@dataclass
class ESQueryForTagsAndOwner(SearchQuery):
    def get_query(key: str):
        key = key + ".keyword"
        return {
            "_source": False,
            "aggs": {
                "distinct": {
                    "terms": {
                        "field": key
                    }
                }
            }
        }


@typechecked
@dataclass
class ESSuperQueryForFeatureSearchWithVersion(SearchQuery):
    def get_query(name: str, version: int):
        filters = []
        if version == -1:
            version = 1
        filters.append({
            "term": {
                "version": version
            }})

        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "regexp": {
                                "name": name
                            }
                        }
                    ],
                    "filter": filters
                }
            }
        }

        return query
