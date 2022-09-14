from typeguard import typechecked
from typeguard import typechecked
from mlp_monitoring.constant.constants import ERROR, SUCCESS
from mlp_monitoring.dao.mysql_dao import MySQLDao
from mlp_monitoring.db_queries.queries import INSERT_CLUSTER_ACTION, \
    DELETE_CLUSTER_ACTIONS


@typechecked
class ClusterActions:
    query: str
    data: tuple

    def __init__(self, env: str):
        self.dao = MySQLDao(env)

    def insert_cluster_action(self, cluster_runid: str, action: str, message: str, cluster_id: str, artifact_id: str):
        """
        Add entry to cluster actions table
        :param cluster_runid: Run id of cluster
        :param action: Type of action being executed by cluster
        :param message: Description of the action
        :param cluster_id: Id of cluster to add
        :param artifact_id: Artifact id of cluster
        :return:
            success: No. of rows inserted
            failure: Error
        """
        self.query = INSERT_CLUSTER_ACTION
        self.data = (cluster_runid, action, message, cluster_id, artifact_id)
        result = self.dao.insert(self.query, self.data)
        if type(result) is int:
            return {
                "status": SUCCESS,
                "data": result
            }
        else:
            return {
                "status": ERROR,
                "data": result
            }

    def delete_cluster_actions (self, cluster_id: str):
        """
        Delete entries from actions table for a cluster
        :param cluster_id: cluster id for which entries need to be deleted
        :return:
        """
        self.query = DELETE_CLUSTER_ACTIONS % cluster_id
        result = self.dao.delete(self.query)
        if type(result) is int:
            return {
                "status": SUCCESS,
                "data": result
            }
        else:
            return {
                "status": ERROR,
                "data": result
            }
