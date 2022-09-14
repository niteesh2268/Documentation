import yaml
from mlp_commons.dao.elastic_search_dao import ElasticSearchDao

from compute_sdk.constant.config import Config
from compute_sdk.constant.constants import ELASTIC_INDEX_MAPPING, ERROR, INDEX
from compute_sdk.entities.request.compute_request import ComputeDefinition, ComputeSearchQuery
import shortuuid

def create_mapping(env):
    try:
        es_dao = ElasticSearchDao(Config(env).get_elastic_url, INDEX, lambda x: ComputeDefinition.from_dict(x))
        return es_dao.create_indice(ELASTIC_INDEX_MAPPING)
    except Exception as e:
        return {'status': ERROR, 'data': e}


def search_query(search_keyword, sort_by, sort_order,offset,page_size):
    query = ".*" + search_keyword + ".*"
    return ComputeSearchQuery({
        "query": {
            "bool": {
                "should": [
                    {
                        "regexp": {
                            "name": {
                                "value": query
                            }
                        }
                    },
                    {
                        "regexp": {
                            "tags": {
                                "value": query
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                sort_by: {
                    "order": sort_order
                }
            }
        ],
        "from": offset, "size": page_size
    })


def match_all_query():
    return ComputeSearchQuery({"query": {"match_all": {}}})


def agg_query(field):
    agg_field = field + ".keyword"
    return ComputeSearchQuery({
        "_source": False,
        "aggs": {
            "distinct": {
                "terms": {
                    "field": agg_field
                }
            }
        }
    })


def search_cluster_name(name):
    return ComputeSearchQuery({
        "query": {
            "bool": {
                "should": [
                    {
                        "term": {
                            "name": {
                                "value": name
                            }
                        }
                    }
                ]
            }
        }
    })


def read_yaml(file_path: dict):
    with open(file_path, 'r') as stream:
        try:
            d = yaml.safe_load(stream)
            return d
        except yaml.YAMLError as e:
            return e


def get_random_id():
    shortuuid.set_alphabet('23456789abcdefghijkmnopqrstuvwxyz')
    rand_id = "id-" + shortuuid.random()
    return rand_id

