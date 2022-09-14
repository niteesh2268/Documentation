from typeguard import typechecked
from mlp_monitoring.constant.constants import ERROR, SUCCESS
from mlp_monitoring.dao.mysql_dao import MySQLDao
from mlp_monitoring.db_queries.queries import GET_ALL_CLUSTERS_STATUS, \
    INSERT_CLUSTER_STATUS, \
    UPDATE_CLUSTER, \
    GET_CLUSTER_LAST_UPDATED, \
    DELETE_CLUSTER, \
    UPDATE_CLUSTER_STATUS, \
    UPDATE_CLUSTER_ARTIFACT, \
    UPDATE_CLUSTER_URLS, \
    UPDATE_CLUSTER_RUN_ID, \
    UPDATE_CLUSTER_NAME, \
    GET_CLUSTERS_FROM_LIST
from mlp_monitoring.utils.date_serializer import serialize_date


@typechecked
class ClusterStatus:
    query: str
    data: tuple

    def __init__(self, env: str):
        self.dao = MySQLDao(env)

    def get_all_clusters_status(self):
        """
        Returns all clusters in table
        :return:
            success: list of rows from cluster_status(id, cluster_id, artifact_id, status, last_updated_at) table
            failure: error
        """
        self.query = GET_ALL_CLUSTERS_STATUS
        result = self.dao.retrieve(self.query)
        if not type(result) is str:
            for x in result:
                x['last_updated_at'] = serialize_date(x['last_updated_at'])
            return {
                "status": SUCCESS,
                "data": result
            }
        else:
            return {
                "status": ERROR,
                "data": result
            }

    def insert_cluster_status(self, cluster_id: str, artifact_id: str, cluster_name: str):
        """
        Add entry to cluster_status table
        :param cluster_id: Id of cluster to add
        :param artifact_id: Artifact id of cluster
        :param cluster_name: Cluster name to add
        :return:
            success: No. of rows inserted
            failure: error
        """
        self.query = INSERT_CLUSTER_STATUS
        self.data = (cluster_id, artifact_id, 'inactive', cluster_name)
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

    def update_cluster(self, cluster_id: str, artifact_id: str, status: str, active_pods: int,
                       available_memory: int, active_cluster_run_id: str):
        """
        Update row for cluster
        :param cluster_id: Id of cluster to update
        :param artifact_id: Artifact id of cluster
        :param status: ('active', 'inactive', 'creating') status of cluster
        :param active_pods: Pods active in the cluster
        :param available_memory: Total available memory in the cluster
        :param active_cluster_run_id: Active run_id for a cluster
        :return:
            success: No. of rows updated
            failure: error
        """
        self.query = UPDATE_CLUSTER % (status, active_pods, available_memory, active_cluster_run_id,
                                       artifact_id, cluster_id)
        result = self.dao.update(self.query)
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

    def update_cluster_status(self, cluster_id: str, status: str):
        """
        Update row for cluster
        :param cluster_id: Id of cluster to update
        :param status: ('active', 'inactive', 'creating') status of cluster
        :return:
            success: No. of rows updated
            failure: error
        """
        self.query = UPDATE_CLUSTER_STATUS % (status, cluster_id)
        result = self.dao.update(self.query)
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

    def update_cluster_artifact(self, cluster_id: str, artifact_id: str):
        """
        Update row for cluster
        :param cluster_id: Id of cluster to update
        :param artifact_id: Artifact id of cluster
        :return:
            success: No. of rows updated
            failure: error
        """
        self.query = UPDATE_CLUSTER_ARTIFACT % (artifact_id, cluster_id)
        result = self.dao.update(self.query)
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

    def update_cluster_run_id(self, run_id: str, cluster_id: str):
        """
        Update last active run id for cluster
        :param run_id: Current active run id
        :param cluster_id: Cluster id to update
        :return:
            success: No. of rows updated
            failure: error
        """
        self.query = UPDATE_CLUSTER_RUN_ID % (run_id, cluster_id)
        result = self.dao.update(self.query)
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

    def update_cluster_name(self, cluster_id: str, cluster_name: str):
        """
        Updates cluster name in status table
        :param cluster_id: Cluster id to update
        :param cluster_name: Cluster name to update with
        :return:
            success: No. of rows inserted
            failure: error
        """
        self.query = UPDATE_CLUSTER_NAME % (cluster_name, cluster_id)
        result = self.dao.update(self.query)
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

    def update_urls(self, dashboard_url: str, notebook_url: str, cluster_id: str):
        """
        Updates row with ray dashboard url and jupyter notebook url of cluster
        :param dashboard_url: link to ray dashboard
        :param notebook_url: link to jupyter notebook
        :param cluster_id: Cluster id to updated
        :return:
            success: No. of rows updated
            faiure: Error
        """
        self.query = UPDATE_CLUSTER_URLS % (dashboard_url, notebook_url, cluster_id)
        result = self.dao.update(self.query)
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

    def get_latest_cluster_info(self, cluster_id: str):
        """
        Retrieves latest state of cluster for cluster_id
        :param cluster_id: Cluster id whose latest info needs to be fetched
        :return:
            success: Returns latest cluster info
        """
        self.query = GET_CLUSTER_LAST_UPDATED % cluster_id
        result = self.dao.retrieve(self.query)
        if not type(result) is str:
            if len(result) == 0:
                return {
                    "status": ERROR,
                    "data": "Cluster does not exist"
                }
            for x in result:
                x['last_updated_at'] = serialize_date(x['last_updated_at'])
            return {
                "status": SUCCESS,
                "data": result[0]
            }
        else:
            return {
                "status": ERROR,
                "data": result
            }

    def delete_cluster(self, cluster_id: str):
        """
        Deletes the cluster with given cluster_id
        :param cluster_id: Cluster id which needs to be deleted
        :return:
            success:
        """
        self.query = DELETE_CLUSTER % cluster_id
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

    def get_clusters_from_list(self, cluster_list: list):
        """
        Retrieve a list of clusters
        :param cluster_list: list of cluster_ids to retrieve
        :return: List of clusters
        """
        if len(cluster_list) > 1:
            self.query = GET_CLUSTERS_FROM_LIST + str(tuple(cluster_list))
        else:
            self.query = GET_CLUSTERS_FROM_LIST + str(tuple(cluster_list)).replace(",)", ")")
        result = self.dao.retrieve(self.query)
        if type(result) is list:
            return {
                "status": SUCCESS,
                "data": result
            }
        else:
            return {
                "status": ERROR,
                "data": result
            }
