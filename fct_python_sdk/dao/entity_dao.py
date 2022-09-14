from typing import List

from mlp_commons.dao.elastic_search_dao import ElasticSearchDao
from mlp_commons.dao.generic_dao import Dao
from mlp_commons.models.dao_response import DaoResponse
from typeguard import typechecked
from typing_extensions import Literal
from fct_python_sdk.config.all_config_values import CONFIGS_MAP
from fct_python_sdk.entities.entity.entity_definition import EntityDefinition, EntityIdentifier

@typechecked
class EntityDao:
    """
    DAO for managing Entities
    """

    def __init__(self, env: Literal["prod", "stag", "test", "local"]):
        config = CONFIGS_MAP[env]["datastore.configs"]

        self.dao: Dao[EntityDefinition, EntityIdentifier] = ElasticSearchDao[EntityDefinition, EntityIdentifier](
            config["elastic-search.url"], config["elastic-search-fct-index"],
            lambda raw_dict: EntityDefinition.from_dict(raw_dict),
            config["elastic-search.id"],
            config["elastic-search.pass"]
        )

    def create(self, name: str, value_type: str, join_keys: List[str], description: str) -> DaoResponse:
        return self.dao.create(EntityDefinition(name, value_type, join_keys, description), EntityIdentifier(name))

    def get(self, name: str) -> DaoResponse:
        return self.dao.read(EntityIdentifier(name))

    def filter_entities_in_db(self, entities: List[str]) -> List[str]:
        unregistered_entities = []
        print("Checking entity validation")
        for entity in entities:
            if self.get(entity).status == "ERROR":
                unregistered_entities.append(entity)

        return unregistered_entities

