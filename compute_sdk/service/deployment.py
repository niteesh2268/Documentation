import requests
from mlp_monitoring.cluster_status import ClusterStatus

from compute_sdk.constant.config import Config
from compute_sdk.constant.constants import CLUSTER_CREATE_URL
from compute_sdk.constant.constants import CLUSTER_START_URL, CLUSTER_STOP_URL, CLUSTER_RESTART_URL, ERROR, SUCCESS
from compute_sdk.entities.request.compute_request import ComputeDefinition
from compute_sdk.helper import yaml_generator


def _get_url(env):
    return Config(env).get_compute_url

# Here cluster_name is cluster_id


class Deployment:
    def __init__(self, env='stag'):
        self.url = _get_url(env)
        self.cluster_status = ClusterStatus(env)

    def create_cluster(self, cluster_id: str, compute_request: ComputeDefinition):
        version = 1
        artifact_name = self.get_artifact_name(cluster_id, version)
        cluster_name = compute_request.name
        self.cluster_status_insert(cluster_id, artifact_name,cluster_name)
        params = {'cluster_name': cluster_id, "artifact_name": artifact_name}
        config_file = {'file': self.generate_config_file(compute_request)}
        response = requests.request("POST", self.url + CLUSTER_CREATE_URL, headers={}, data=params, files=config_file)
        if (response.status_code >= 200 and response.status_code < 300):
            return response.json()
        raise Exception(response.text)

    def update_cluster(self, cluster_id: str, compute_request: ComputeDefinition):
        new_version = self.get_latest_version(cluster_id) + 1
        artifact_name = self.get_artifact_name(cluster_id, new_version)
        params = {'cluster_name': cluster_id, "artifact_name": artifact_name}
        config_file = {'file': self.generate_config_file(compute_request)}
        cluster_name = compute_request.name
        response = requests.request("PUT", self.url + CLUSTER_CREATE_URL, headers={}, data=params, files=config_file)
        status = self.get_cluster_status(cluster_id)['status']
        if (response.status_code >= 200 and response.status_code < 300):
            self.update_cluster_status(cluster_id, artifact_name, status,cluster_name)
            return response.json()
        raise Exception(response.text)

    def update_cluster_and_apply(self, cluster_id: str, compute_request: ComputeDefinition):
        new_version = self.get_latest_version(cluster_id) + 1
        artifact_name = self.get_artifact_name(cluster_id, new_version)
        cluster_name = compute_request.name
        params = {'cluster_name': cluster_id, "artifact_name": artifact_name}
        config_file = {'file': self.generate_config_file(compute_request)}
        response = requests.request("PUT", self.url + CLUSTER_CREATE_URL, headers={}, data=params, files=config_file)
        if (not (response.status_code >= 200 and response.status_code < 300)):
            raise Exception(response.text)
        self.update_cluster_status(cluster_id, artifact_name, 'creating',cluster_name)
        return self.start_cluster(cluster_id)

    def start_cluster(self, cluster_id: str):
        version = self.get_latest_version(cluster_id)
        artifact_name = self.get_artifact_name(cluster_id, version)
        params = {'cluster_name': cluster_id, "artifact_name": artifact_name}
        response = requests.request("PUT", self.url + CLUSTER_START_URL, headers={}, data=params)
        if (response.status_code >= 200 and response.status_code < 300):
            json_resp = response.json()
            resp = self.cluster_status.update_cluster_status(cluster_id, 'creating')
            if resp['status'] == ERROR:
                raise Exception(resp['data'])
            self.update_urls(json_resp['DashboardLink'],json_resp['JupyterLink'],cluster_id)
            return json_resp
        raise Exception(response.text)

    def stop_cluster(self, cluster_id: str):
        version = self.get_latest_version(cluster_id)
        artifact_name = self.get_artifact_name(cluster_id, version)
        params = {'cluster_name': cluster_id}
        response = requests.request("PUT", self.url + CLUSTER_STOP_URL, headers={}, data=params)

        if (response.status_code >= 200 and response.status_code < 300):
            resp = self.cluster_status.update_cluster_status(cluster_id, 'inactive')
            if resp['status'] == ERROR:
                raise Exception(resp['data'])
            return response.text
        raise Exception(response.text)

    def restart_cluster(self, cluster_id: str):
        version = self.get_latest_version(cluster_id)
        artifact_name = self.get_artifact_name(cluster_id, version)
        params = {'cluster_name': cluster_id, "artifact_name": artifact_name}
        response = requests.request("PUT", self.url + CLUSTER_RESTART_URL, headers={}, data=params)

        if (response.status_code >= 200 and response.status_code < 300):
            json_resp = response.json()
            resp = self.cluster_status.update_cluster_status(cluster_id, 'creating')
            if resp['status'] == ERROR:
                raise Exception(resp['data'])
            self.update_urls(json_resp['DashboardLink'], json_resp['JupyterLink'], cluster_id)
            return json_resp
        raise Exception(response.text)

    def generate_config_file(self, compute_definition: ComputeDefinition):
        file = yaml_generator.create_yaml(compute_definition)
        return file


    def get_artifact_name(self, name: str, version: int):
        return name + '-v' + str(version)

    def get_latest_version(self, cluster_id: str):
        resp = self.cluster_status.get_latest_cluster_info(cluster_id)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])
        cluster_row = resp['data']
        if (cluster_row is not None and 'artifact_id' in cluster_row):
            artifact_id = cluster_row['artifact_id']
            if (artifact_id is not None):
                return int(artifact_id[artifact_id.rfind('-v') + 2:])
        return 1

    def cluster_status_insert(self, cluster_id: str, artifact_name: str, cluster_name: str):
        resp = self.cluster_status.insert_cluster_status(cluster_id, artifact_name, cluster_name)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])

    def get_cluster_status(self, cluster_id: str):
        resp = self.cluster_status.get_latest_cluster_info(cluster_id)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])
        return resp['data']

    def update_cluster_status(self, cluster_id: str, artifact_name: str, status: str,cluster_name:str):
        resp = self.cluster_status.update_cluster_status(cluster_id, status)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])
        resp = self.cluster_status.update_cluster_artifact(cluster_id, artifact_name)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])
        resp = self.cluster_status.update_cluster_name(cluster_id, cluster_name)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])

    def update_urls(self, dashboard_url: str, jupyter_url: str,cluster_id: str):
        resp = self.cluster_status.update_urls(dashboard_url,jupyter_url,cluster_id)
        if resp['status'] == ERROR:
            raise Exception(resp['data'])
