from typing import TypeVar, List

from fct_python_sdk.entities.fct_response import FCTResponse
from fct_python_sdk.entities.feature_metada import VersionableEntity
from fct_python_sdk.entities.search_query import ElasticSearchQuery, ESQueryForFeatureSearch, ESQueryForTagsAndOwner, \
    CreateIndexMapping
from fct_python_sdk.entities.source.source import Source


def compare_list_if_not_equal(list1, list2) -> bool:
    set1 = set(list1)
    set2 = set(list2)
    return not (set1 == set2)


T = TypeVar('T', bound=VersionableEntity)


def get_create_action(list1: List[T], list2: List[T]):
    for (a, b) in zip(list1, list2):
        if a.get_create_action(b) == "CREATE_NEW_VERSION":
            return "CREATE_NEW_VERSION"
    return "NO_ACTION"


def transform_sources_and_sinks(sources: List[Source]):
    transformed_sources = []
    for source in sources:
        if source.type == "redshift":
            transformed_sources.append({"datasource": "redShift", "location": source.location})
        elif source.type == "s3":
            transformed_sources.append({"datasource": "s3", "location": source.location})

    return transformed_sources


def transform_search(search_result, offset: int, limit: int):
    feature_groups = []

    if search_result is not None and search_result.data is not None:
        for res in search_result.data:
            feature_groups.append({
                "name": res.name,
                "description": res.description,
                "createdDate": res.created_at,
                "ownerEmail": res.user,
                "status": res.status,
                "tags": res.tags,
                "version": res.version
            })

    results = {
        "offset": offset,
        "pageSize": limit,
        "totalResults": len(feature_groups),
        "featureGroups": feature_groups
    }
    return results


def get_feature_group_latest_version_details_result(feature_groups):
    if len(feature_groups) == 0:
        return {}
    feature_group = feature_groups[0]

    return {
        "name": feature_group.name,
        "version": feature_group.version,
        "tags": feature_group.tags,
        "description": feature_group.description,
        "ownerEmail": feature_group.user,
        "slackNotificationChannel": feature_group.notification_channel,
        "sources": transform_sources_and_sinks(feature_group.sources),
        "sinks": transform_sources_and_sinks(feature_group.sinks),
        "features": feature_group.schema,
        "computeMetadata": feature_group.compute_meta_data,
        "status": feature_group.status,
        "createdAt": feature_group.created_at
    }


def get_feature_group_definition(self, name: str):
    try:
        search_result = self.dao.search(ElasticSearchQuery(ESQueryForFeatureSearch.get_query(name)))
        if search_result.status != "ERROR":
            return FCTResponse.success_response("SEARCH", search_result.data)
        else:
            raise Exception(search_result.message)
    except Exception as e:
        return FCTResponse.error_response("SEARCH", str(e))


def get_distinct_keywords(self, key: str):
    try:
        distinct_keys = []
        search_result = self.dao.aggregation_search(ElasticSearchQuery(ESQueryForTagsAndOwner.get_query(key)))
        distinct = search_result.data["aggregations"]["distinct"]["buckets"]

        for key in distinct:
            distinct_keys.append(key["key"])
        if search_result.status != "ERROR":
            return FCTResponse.success_response("SEARCH", distinct_keys)
        else:
            raise Exception(search_result.message)
    except Exception as e:
        return FCTResponse.error_response("SEARCH", str(e))


def create_mapping(self):
    try:
        search_result = self.dao.create_indice(CreateIndexMapping.get_query())
        if search_result.status != "ERROR":
            return FCTResponse.success_response("SEARCH", search_result.data)
        else:
            raise Exception(search_result.message)
    except Exception as e:
        return FCTResponse.error_response("SEARCH", str(e))


