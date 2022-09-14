from typeguard import typechecked
from fct_python_sdk.consumption.stores import OfflineStore
from fct_python_sdk.entities.feature_group import FeatureGroupDefinition


@typechecked
class FeatureGroup:
    """
    A feature group object represents a registered list of features which can be read and written at the same time.
    This has a feature group definition which is registered.
    """

    definition: FeatureGroupDefinition
    offline_store: OfflineStore

    def __init__(self, definition: FeatureGroupDefinition):
        if definition is None:
            raise ValueError("Feature group definition is found to be null")

        self.definition = definition
        self.offline_store = OfflineStore(definition.sinks[0])

    def offline_store(self):
        return self.offline_store
