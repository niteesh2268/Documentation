from typing import Optional, List
from typing_extensions import Literal
from delta import DeltaTable
from typeguard import typechecked

from fct_python_sdk.entities.sink.sink import Sink

MODE = Literal['overwrite', 'append']


@typechecked
class OfflineStore:
    """
    A handler to the offline store where the features are stored.
    Offline store supports batch reads and batch writes
    """

    sink: Sink

    def __init__(self, sink: Sink):
        if Sink is None:
            raise ValueError("Sink is found to be null")

        self.sink = sink

    def push_data_using_spark(self, spark_df, mode: MODE = 'overwrite'):
        spark_df.write.format("delta").mode(mode).save(self.sink.location)

    def push_data_using_ray(self, ray_context, ray_df, mode: MODE = 'overwrite'):
        raise NotImplementedError

    def get_runs(self, spark_context, limit=10):
        delta_table = DeltaTable.forPath(spark_context, self.sink.location)
        return delta_table.history(limit)

    def get_features(self, features: Optional[List[str]] = None, delta_version: Optional[int] = None):
        return RetrieveFGRequest(self.sink, features, delta_version, None)

    def get_features_as_of_time(self, features: Optional[List[str]] = None, timestamp: Optional[str] = None):
        return RetrieveFGRequest(self.sink, features, None, timestamp)


@typechecked
class RetrieveFGRequest:
    """
    An object to represent a retrivable feature group
    """

    fg_sink: Sink
    features: Optional[List[str]]
    delta_version: Optional[int]
    timestamp: Optional[str]

    def __init__(self, sink: Sink, features: Optional[List[str]], delta_version: Optional[int], timestamp: Optional[str]):
        if sink is None:
            raise ValueError("Sink is found to be null")

        if timestamp is not None and delta_version is not None:
            raise ValueError("Delta Version and timestamp cant be given at once")

        self.timestamp = timestamp
        self.sink = sink
        self.features = features
        self.delta_version = delta_version

    def to_spark_df(self, spark_context):
        if self.timestamp is not None:
            return self._get_features_for_time_stamp(spark_context, self.features, self.timestamp)
        else:
            return self._get_features_for_version(spark_context, self.features, self.delta_version)

    def to_ray_df(self, ray_context):
        raise NotImplementedError

    def to_pandas_df(self):
        raise NotImplementedError

    def _get_features_for_version(self, spark_context, features: Optional[List[str]], delta_version: Optional[int]):
        if delta_version is None or delta_version < 0:
            delta_version = self._get_latest_delta_version(spark_context)
        if features is None or len(features) == 0:
            return spark_context.read.format('delta').option('versionAsOf', delta_version).load(self.sink.location)
        else:
            return spark_context.read.format('delta').option('versionAsOf', int(delta_version)).load(
                self.sink.location).select(
                features)

    def _get_features_for_time_stamp(self, spark_context, features: Optional[List[str]], timestamp_string: Optional[str]):
        if features is None or len(features) == 0:
            return spark_context.read.format('delta').option("timestampAsOf", timestamp_string).load(
                self.sink.location)
        else:
            return spark_context.read.format('delta').option("timestampAsOf", timestamp_string).load(
                self.sink.location).select(features)

    def _get_latest_delta_version(self, spark_context):
        delta_table = DeltaTable.forPath(spark_context, self.sink.location)
        df =  delta_table.history(1)
        return df.collect()[0]['version']


@typechecked
class RetrievableDataSet:
    """
    An object to represent a dataset which can be retrived from offline feature store
    """

    retrieval_request_list: List[RetrieveFGRequest]

    def __init__(self, features_list: List[str]):
        if features_list is None:
            raise ValueError("features_list is found to be null")

        self.features_list = features_list
