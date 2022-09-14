from typeguard import typechecked
import datetime
from dateutil.tz import gettz

from compute_sdk.constant.constants import SUCCESS, RUNTIMES, TEMPLATES, DISK_TYPE, INSTANCE_ROLE, TAGS_FIELD, AZS, \
    ALREADY_EXIST
from compute_sdk.entities.request.compute_request import ComputeIdentifier
from compute_sdk.helper.helper import *
from compute_sdk.service.deployment import Deployment
from compute_sdk.entities.response.entity import ClusterIdentifier

def _get_elasticsearch_instance(path):
    return ElasticSearchDao(path, INDEX, lambda x: ComputeDefinition.from_dict(x))


class Compute:
    def __init__(self, env):
        self.es_dao = _get_elasticsearch_instance(Config(env).get_elastic_url)
        self.deployment = Deployment(env)

    @typechecked()
    def search_cluster_name(self, name: str):
        return self.es_dao.aggregation_search(search_cluster_name(name))

    @typechecked
    def create_cluster_with_yaml(self, file_path: str):
        """
        Create cluster suing configuration stored in yaml file

        :param file_path: File path containing Cluster definition details
        :return: Return the status of cluster creation and cluster_id

        """
        try:
            req_dict = read_yaml(file_path)
            compute_request = ComputeDefinition.from_dict(req_dict)
            return self.create_cluster(compute_request)
        except Exception as e:
            return {'status': ERROR, 'data': e}

    @typechecked
    def create_cluster(self, compute_request: ComputeDefinition):
        """
        Creates a cluster using compute definition
        :param compute_request: Cluster definition object
        :return: Return the status of cluster creation and cluster_id

        """
        try:
            resp = self.search_cluster_name(compute_request.name)
            if resp.status == ERROR:
                return resp
            if len(resp.data["hits"]["hits"]) != 0:
                raise Exception(ALREADY_EXIST)
            cluster_id = get_random_id()
            compute_request.cluster_id = cluster_id
            compute_request.created_on = str(datetime.datetime.now(tz=gettz('Asia/Kolkata')))
            dao_resp = self.es_dao.create(compute_request,ClusterIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            create_resp = self.deployment.create_cluster(dao_resp.message, compute_request)
            return {'status':SUCCESS, 'data' : {'cluster_id' : create_resp['ClusterName']}}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def delete_cluster(self, cluster_id):
        """
        Deletes a cluster for a given a cluster_id
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :return: Returns the status of the request and cluster_id

        """
        try:
            if self.deployment.get_cluster_status(cluster_id)['status'] == "Active" :
                raise ValueError("Cluster is running")
            resp = self.deployment.cluster_status.delete_cluster(cluster_id)
            if resp['status'] == ERROR:
                return resp
            dao_resp = self.es_dao.delete(ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            return {'status' : SUCCESS, 'data':None}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def get_cluster(self, cluster_id):
        """
        Fetch cluster details
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :return: Returns the status of the request and cluster_id

        """
        return self.es_dao.read(ComputeIdentifier(cluster_id))

    @typechecked
    def update_cluster(self, cluster_id: str, compute_request: ComputeDefinition):
        """
        Updates the cluster details
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :param compute_request: Path of the yaml file containing configurations for the cluster
        :return: Returns the status of the request and cluster_id

        """
        try:
            compute_request.cluster_id=cluster_id
            dao_resp = self.es_dao.read(ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            compute_request.user = dao_resp.data.user
            compute_request.created_on = dao_resp.data.created_on
            dao_resp = self.es_dao.update(compute_request, ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            create_resp = self.deployment.update_cluster(dao_resp.message, compute_request)
            return {'status':SUCCESS,'data':{'cluster_id' : create_resp['ClusterName']}}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def update_cluster_with_yaml(self, cluster_id: str, file_path: str):
        """
        Updates the cluster details
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :param compute_request: Path of the yaml file containing configurations for the cluster
        :return: Returns the status of the request and cluster_id

        """
        try:
            req_dict = read_yaml(file_path)
            compute_request = ComputeDefinition.from_dict(req_dict)
            return self.update_cluster(cluster_id,compute_request)
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def start(self, cluster_id):
        """

        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :return: Returns the status of the request and cluster_id

        """
        try:
            resp = self.deployment.start_cluster(cluster_id)
            return {'status':SUCCESS,'data':resp}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def stop(self, cluster_id):
        """

        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :return: Returns the status of the request and cluster_id

        """
        try:
            resp = self.deployment.stop_cluster(cluster_id)
            return {'status':SUCCESS,'data':None}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def restart(self, cluster_id):
        """
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :return: Returns the status of the request and cluster_id
        """
        try:
            resp = self.deployment.restart_cluster(cluster_id)
            return {'status':SUCCESS,'data':resp}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    @typechecked
    def update_and_apply_changes(self, cluster_id: str, new_configuration: ComputeDefinition):
        """
        Update the cluster definition and restart the cluster
        :param cluster_id: cluster_id of the cluster needs to be updated
        :param new_configuration:
        :return:
        """
        try:
            new_configuration.cluster_id = cluster_id
            dao_resp = self.es_dao.read(ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            new_configuration.user = dao_resp.data.user
            new_configuration.created_on = dao_resp.data.created_on
            dao_resp = self.es_dao.update(new_configuration, ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            resp = self.deployment.update_cluster_and_apply(cluster_id, new_configuration)
            return {'status':SUCCESS,'data':resp}
        except Exception as e:
            return {'status': ERROR, 'data': e}

    def update_and_apply_changes_with_yaml(self, cluster_id: str, file_path: str):
        """
        Updates the cluster details
        :param cluster_id: Cluster identification of the cluster which needs to be updated
        :param compute_request: Path of the yaml file containing configurations for the cluster
        :return: Returns the status of the request and cluster_id

        """
        try:
            req_dict = read_yaml(file_path)
            compute_request = ComputeDefinition.from_dict(req_dict)
            return self.update_and_apply_changes(cluster_id,compute_request)
        except Exception as e:
            return {'status': ERROR, 'data': e}

    @typechecked
    def update_cluster_name(self, cluster_id: str, cluster_name: str):
        """
        Updates only the cluster name
        :param cluster_id: cluster_id of the cluster needs to be updated
        :param cluster_name: new cluster name
        :return: cluster id and status of the request

        """
        try:
            resp = self.search_cluster_name(cluster_name)
            if resp.status == ERROR:
                return resp
            if len(resp.data["hits"]["hits"]) != 0:
                raise Exception(ALREADY_EXIST)
            dao_resp = self.es_dao.read(ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            dao_resp.data.name = cluster_name
            return self.es_dao.update(dao_resp.data, ComputeIdentifier(cluster_id))
        except Exception as e:
            return {'status': ERROR, 'data': e}

    @typechecked
    def update_cluster_tags(self, cluster_id: str, tags: list):
        """
        Updates only the cluster tags
        :param cluster_id: cluster_id of the cluster needs to be updated
        :param tags: new tag list
        :return: cluster id and status of the request

        """
        try:
            dao_resp = self.es_dao.read(ComputeIdentifier(cluster_id))
            if dao_resp.status == ERROR:
                return dao_resp
            dao_resp.data.tags = tags
            return self.es_dao.update(dao_resp.data, ComputeIdentifier(cluster_id))
        except Exception as e:
            return {'status': ERROR, 'data': e}

    # TODO aggregation query  response
    @typechecked
    def search_cluster(self, search_keyword: str, filters: str, page_size: int, offset: int, sort_by: str,
                       sort_order: str):
        """
        Search function
        :param search_keyword: keyword to be searched
        :param filters: filter on status of the cluster
        :param page_size: page size for search result
        :param offset: offset
        :param sort_by: sort on
        :param sort_order: order of sort
        :return: list of clusters based on filter and sorting order provided

        """
        return self.es_dao.search(search_query(search_keyword, sort_by, sort_order,offset,page_size))

    def get_runtimes(self):
        """

        :return: list of runtimes available

        """
        return {
            "status": SUCCESS,
            "data": RUNTIMES
        }

    def get_templates(self):
        """

        :return: list of templates for cluster

        """
        return {
            "status": SUCCESS,
            "data": TEMPLATES
        }

    def get_disk_types(self):
        """

        :return: list of templates for cluster

        """
        return {
            "status": SUCCESS,
            "data": DISK_TYPE
        }

    def get_instance_role(self):
        """

        :return:

        """
        return {
            "status": SUCCESS,
            "data": INSTANCE_ROLE
        }

    def get_az(self):
        """

        :return:

        """
        return {
            "status": SUCCESS,
            "data": AZS
        }

    def get_tags(self):
        """

        :return: list of tags from all compute

        """
        dao_resp = self.es_dao.aggregation_search(agg_query(TAGS_FIELD))
        if dao_resp.status == ERROR:
            return dao_resp
        tag_list = [x["key"] for x in dao_resp.data["aggregations"]["distinct"]["buckets"]]
        return {
            "status": SUCCESS,
            "data": tag_list
        }

    def get_cluster_status(self, cluster_id: str):
        try:
            resp = self.deployment.get_cluster_status(cluster_id)
        except Exception as e:
            return {
                "status": ERROR,
                "data": e
            }
        return {
            "status": SUCCESS,
            "data": resp
        }

    def get_all_cluster_status(self):
        return self.deployment.cluster_status.get_all_clusters_status()
