import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from mlp_commons.dao.elastic_search_dao import ElasticSearchDao
from mlp_commons.dao.generic_dao import Dao
from typeguard import typechecked
from typing_extensions import Literal

from fct_python_sdk.dao.entity_dao import EntityDao
from fct_python_sdk.config.all_config_values import CONFIGS_MAP
from fct_python_sdk.constants.constants import ENV_TYPE
from fct_python_sdk.consumption.retrieved_feature_group import FeatureGroup
from fct_python_sdk.entities.ComputeMetaData import ComputeMetaData
from fct_python_sdk.entities.fct_response import FCTResponse
from fct_python_sdk.entities.feature import Feature
from fct_python_sdk.entities.feature_group import FeatureGroupDefinition, FgIdentifier
from fct_python_sdk.entities.search_query import ESSuperQueryForFeatureSearch, ESSuperQueryForFeatureSearchWithVersion, \
    ElasticSearchQuery, ESQueryForTagsAndOwner, ESQueryForFeatureSearch
from fct_python_sdk.entities.sink.sink import Sink
from fct_python_sdk.entities.source.source import Source
from fct_python_sdk.utils.utils import get_feature_group_latest_version_details_result, transform_search


@dataclass
@typechecked
class FCT:
    """
               User facing class for accessing the functionalities of Feature Control Tower.

               Usage:
               from fct_python_sdk.fct import FCT
               sdk = FCT("prod")
    """

    # env: ENV_TYPE

    def __init__(self, env: ENV_TYPE):
        """
                Creates an FCT object.Valid values for env are ["prod", "stag", "test", "local"]

               :param env: String representing the environment or domain in which we want to use FCT.

        """
        config = CONFIGS_MAP[env]["datastore.configs"]
        self.env = env
        self.dao: Dao[FeatureGroupDefinition, FgIdentifier] = ElasticSearchDao[FeatureGroupDefinition, FgIdentifier](
            config["elastic-search.url"], 'local',
            lambda raw_dict: FeatureGroupDefinition.from_dict(raw_dict),
            config["elastic-search.id"],
            config["elastic-search.pass"]
        )
        self.entity_dao = EntityDao(env)
        self.sink_base_path = CONFIGS_MAP[env]["fg.sink.s3.base_path"]

    def register_feature_group(self,
                               name: str,
                               entities: List[str],
                               sources: List[Source],
                               schema: List[Feature],
                               sink: Optional[Sink],
                               compute_meta_data: ComputeMetaData,
                               tags: List[str],
                               description: str,
                               user: str,
                               notification_channel: List[str],
                               status: Literal["ACTIVE", "INACTIVE"]
                               ):
        """
         Register the feature group definition with FCT.
         If the feature group already exists with the same name an error will be thrown.

        :param name: Unique name of the feature group
        :param entities: List of Strings representing the entities of the feature group
        :param sources: list of sources in the feature group
        :param schema: List of Features which are part of this feature group.
        :param sink: Sink for the feature group. If not defined a new S3 sink is generated
        :param compute_meta_data: compute details associated with the feature group
        :param user: user who is owner of the feature group
        :param tags: tags associated with the feature group
        :param description: tags associated with the feature group
        :param notification_channel: notification_channel associated with the feature group
        :param status: whether a feature group is active or not

        :return: Feature Group Object which has feature group definition and store details
        """
        try:
            print(f"Checking for existing feature groups with name {name}")
            existing_feature_group = self.dao.search(ElasticSearchQuery(ESQueryForFeatureSearch.get_query(name)))

            if existing_feature_group.status == "ERROR":
                raise Exception(existing_feature_group.message)
            if existing_feature_group is None or existing_feature_group.data is None or len(
                    existing_feature_group.data) == 0:
                print(f"Registering a new feature group with the name {name} and version 1")
                feature_group_definition = FeatureGroupDefinition(name, status, datetime.now(), compute_meta_data, 1,
                                                                  self._get_sinks(sink, name, 1), tags,
                                                                  description, entities, user, notification_channel,
                                                                  sources, schema)
                print("Registering feature group definition")
                self._create_feature_group_by_def(feature_group_definition, name, 1)
                print("Feature definition registerd with name", name)
                return FCTResponse.success_response("CREATE", FeatureGroup(feature_group_definition))
            else:
                print(f"[ERROR] Feature Group already exists with the name {name}.")
                return FCTResponse.error_response('CREATE', "Feature Group already exists")
        except Exception as e:
            print(f"[ERROR] Error occurred while registering the feature group {name}.")
            print(traceback.format_exc())
            return FCTResponse.error_response('CREATE', str(e))

    def update_feature_group(self,
                             name: str,
                             entities: List[str],
                             sources: List[Source],
                             schema: List[Feature],
                             sink: Optional[Sink],
                             compute_meta_data: ComputeMetaData,
                             tags: List[str],
                             description: str,
                             user: str,
                             notification_channel: List[str],
                             status: Literal["ACTIVE", "INACTIVE"]
                             ) -> object:
        """
         Update a feature group. There is versioning for feature groups which is maintained at FCT.
         If a feature group does not exist with the same name then an error is thrown. You can create a new FG using register_feature_group_method.

         If a feature group exists with the same name then there can be 2 scenarios:
            1. A new version is created if any one of [schema, sources, entities, sinks] is different
            2. Else, the existing feature group is updated

        :param name: Unique name of the feature group
        :param entities: List of Strings representing the entities of the feature group
        :param sources: list of sources in the feature group
        :param schema: List of Features which are part of this feature group.
        :param sink: Sink for the feature group. If not defined a new S3 sink is generated
        :param compute_meta_data: compute details associated with the feature group
        :param user: user who is owner of the feature group
        :param tags: tags associated with the feature group
        :param description: tags associated with the feature group
        :param notification_channel: notification_channel associated with the feature group
        :param status: whether a feature group is active or not

        :return: Feature Group Object which has feature group definition and store details
        """
        try:
            existing_feature_groups = self._get_feature_group_definition(name).data
            if existing_feature_groups is None or len(existing_feature_groups) == 0:
                print(f"[ERROR] Feature Group doesn't exists with the name {name}."
                      f" Please use register_feature_group to create a new FG")
                return FCTResponse.error_response('UPDATE', "Feature Group already exists")
            else:
                latest_of_existing_fg = existing_feature_groups[0]
                version: int = latest_of_existing_fg.version
                new_feature_group = FeatureGroupDefinition(name, status, datetime.now(), compute_meta_data,
                                                           version, self._get_sinks(sink, name, version), tags,
                                                           description,
                                                           entities, user, notification_channel, sources, schema)

                if latest_of_existing_fg.get_create_action(new_feature_group) == "CREATE_NEW_VERSION":
                    new_version: int = version + 1
                    print(f"Creating a new Version for the feature group with name {name} "
                          f"and new version {new_version}")
                    new_feature_group.version = new_version
                    action = "CREATE"
                    updated_feature_group = FeatureGroupDefinition(name, status, datetime.now(), compute_meta_data,
                                                                   new_version,
                                                                   self._get_sinks(sink, name, new_version), tags,
                                                                   description,
                                                                   entities, user, notification_channel, sources,
                                                                   schema)
                    dao_resp = self._create_feature_group_by_def(updated_feature_group, name, new_version)
                else:
                    new_version: int = version
                    print(f"Updating the details of feature group with name {name} "
                          f"and new version {new_version}")
                    action = "UPDATE"
                    dao_resp = self._create_feature_group_by_def(new_feature_group, name, new_version)

                print("Feature group", dao_resp)
                if dao_resp.status != "ERROR":
                    return FCTResponse.success_response(action, FeatureGroup(dao_resp.data))
                else:
                    raise Exception(dao_resp.message)
        except Exception as e:
            print(f"[ERROR] Error occurred while updating the feature group {name}.")
            print(traceback.format_exc())
            return FCTResponse.error_response('UPDATE', str(e))

    def load_feature_group(self, feature_group_name: str, version: int = -1):
        """
                Load the feature group object to read and write to feature stores.

               :param feature_group_name: Name of the feature group
               :param version: Version which you want to load. It is taken as latest if specified as -1 or None

               :return: FeatureGroup object
        """
        if version is None or version < 0:
            print(f'Get the latest version of feature_group with name {feature_group_name}')
            version = self._get_latest_version_for_fg(feature_group_name)
            print(f'Loading the feature group with name {feature_group_name} and version {version}')

        feature_group_dao_resp = self.dao.read(FgIdentifier(feature_group_name, str(version)))
        if feature_group_dao_resp.status != "ERROR":
            feature_group = FeatureGroup(feature_group_dao_resp.data)
            return feature_group
        else:
            raise Exception(feature_group_dao_resp.message)

    # def register_entity(self, name: str, value_type: str, join_keys: List[str], description: str) -> FCTResponse:
    #     unregistered_entities: List[str] = self.entity_dao.filter_entities_in_db([name])
    #     if len(unregistered_entities) != 0:
    #         dao_resp= self.entity_dao.create(name, value_type, join_keys, description)
    #         if dao_resp.status != "ERROR":
    #             return FCTResponse.success_response('CREATE', dao_resp.data)
    #         else:
    #             raise Exception(dao_resp.message)
    #     else:
    #         raise Exception(f'Entity already exists with the name {name}. Create with different name or reuse the existing one')

    def get_feature_group_latest_version_details(self, name: str):
        """
                To get the details of the latest version of feature group definition with specified name present in the database.

               :param name: name of the feature group
               :return:
        """
        # """
        #         Load the feature group object to read and write to feature stores.
        #
        #        :param feature_group_name: Name of the feature group
        #        :param version: Version which you want to load. It is taken as latest if specified as -1 or None
        #        :return: FeatureGroup object
        # """
        # """
        #         Load the feature group object to read and write to feature stores.
        #
        #        :param feature_group_name: Name of the feature group
        #        :param version: Version which you want to load. It is taken as latest if specified as -1 or None
        #        :return: FeatureGroup object
        # """
        try:
            feature_group_definition_list = self._get_feature_group_definition(name)
            if feature_group_definition_list.status == "ERROR":
                raise Exception(feature_group_definition_list.message)
            result = get_feature_group_latest_version_details_result(feature_group_definition_list.data)
            return FCTResponse.success_response_with_message("READ", result, "Data fetched successfully")
        except Exception as e:
            print(f"[ERROR] Error occurred while getting latest version details of the feature group {name}.")
            print(traceback.format_exc())
            return FCTResponse.error_response("READ", str(e))

    def get_feature_group_definition_by_version(self, feature_group_name: str, version: int):
        """
                Returns the feature group definitions with specified name and version

               :param feature_name: feature group definition
               :param version: version of the feature group definition

               :return: Feature group definition
        """
        try:
            search_result = self.dao.search(
                ElasticSearchQuery(ESSuperQueryForFeatureSearchWithVersion.get_query(feature_group_name, version)))

            transformed_result = None
            if search_result.data is not None and len(search_result.data) != 0:
                transformed_result = search_result.data[0].to_dict()
                transformed_result["features"] = transformed_result["schema"]
                compute_meta_data = transformed_result["compute_meta_data"]
                transformed_result["computeMetadata"] = {"computeType": compute_meta_data["compute_type"],
                                                         "databricksLink": compute_meta_data["link"]}

            if search_result.status != "ERROR":
                return FCTResponse.success_response_with_message("SEARCH", transformed_result,
                                                                 "Data fetched successfully")
            else:
                raise Exception(search_result.message)
        except Exception as e:
            return FCTResponse.error_response("SEARCH", str(e))

    def get_feature_group_version_history(self, name: str):
        """
         To get the details of all the feature group definitions with specified name present in the databse

        :param name: name of the feature group
        :return:
        """
        try:
            feature_group = self._get_feature_group_definition(name)
            return FCTResponse.success_response_with_message("SEARCH", {"feature_groups": feature_group.data},
                                                             "Data fetched successfully")
        except Exception as e:
            return FCTResponse.error_response("SEARCH", str(e))

    def search(self, feature_name: Optional[str] = "", tags: Optional[List[str]] = [], users: Optional[List[str]] = [],
               limit: Optional[int] = 10,
               offset: Optional[int] = 0, sort_by: Optional[str] = "created_at",
               order: Optional[Literal["asc", "desc"]] = "desc"):
        """
        Returns the list of the feature group definitions with specified name (fuzzy search) and other filters

       :param feature_name: feature group query string
       :param tags: List of tags to be filtered
       :param users: List of users to be filtered
       :param limit: Total number of results required
       :param offset:
       :param sort_by: column name to be sorted by
       :param order: param to return results ascending or descending

       :return: List of feature group definitions
        """
        try:
            search_result = self.dao.search(
                ElasticSearchQuery(
                    ESSuperQueryForFeatureSearch.get_query(feature_name, tags, users, limit, offset, sort_by, order)))
            results = transform_search(search_result, offset, limit)
            if search_result.status != "ERROR":
                return FCTResponse.success_response_with_message("SEARCH", results, "Data fetched successfully")
            else:
                raise Exception(search_result.message)
        except Exception as e:
            print(f"[ERROR] Error occurred while getting tags and owners")
            print(traceback.format_exc())
            return FCTResponse.error_response("SEARCH", str(e))

    def get_tags_and_owners(self):
        """
        Returns all the distinct tags and owners present in the database

       :return: {data: {tags: List[str], owners: List[str]}, message: string, action: string}
        """
        try:
            tags = self._get_distinct_keywords("tags")
            owners = self._get_distinct_keywords("user")

            return FCTResponse.success_response_with_message("SEARCH", {
                "tags": tags.data,
                "owners": owners.data
            }, "Data fetched successfully")
        except Exception as e:
            print(f"[ERROR] Error occurred while getting tags and owners")
            print(traceback.format_exc())
            return FCTResponse.error_response("SEARCH", str(e))

    def _get_latest_version_for_fg(self, name: str):
        fct_resp = self.get_feature_group_version_history(name)
        latest_fg_def = fct_resp.get("data")["feature_groups"][0]
        return latest_fg_def.version

    def _get_distinct_keywords(self, key: str):
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

    def _get_feature_group_definition(self, name: str):
        try:
            search_result = self.dao.search(ElasticSearchQuery(ESQueryForFeatureSearch.get_query(name)))
            if search_result.status != "ERROR":
                return FCTResponse.success_response("SEARCH", search_result.data)
            else:
                raise Exception(search_result.message)
        except Exception as e:
            return FCTResponse.error_response("SEARCH", str(e))

    def _create_feature_group_by_def(self, feature_group_definition: FeatureGroupDefinition, name: str,
                                     version: int = 1):
        try:
            unregistered_entities: List[str] = self.entity_dao.filter_entities_in_db(
                feature_group_definition.to_dict()["entities"])

            if len(unregistered_entities) == 0:
                return self.dao.create(feature_group_definition, FgIdentifier(name, str(version)))
            else:
                print(unregistered_entities, f"[ERROR] entities are not registered with us")
                print(traceback.format_exc())
                raise Exception("Entities not registered")
        except Exception as e:
            print(f"[ERROR] Error occurred while getting tags and owners")
            print(traceback.format_exc())
            return FCTResponse.error_response("CREATE", str(e))

    def _get_sinks(self, sink, name, version):
        if sink is None:
            sink = Sink(f'{self.sink_base_path}/{name}/{version}', "s3")
        return [sink]
