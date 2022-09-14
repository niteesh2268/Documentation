import json
import uuid
from unittest import TestCase

from compute_sdk.entities.request.advance_config import AdvanceConfig
from compute_sdk.entities.request.compute_request import ComputeDefinition
from compute_sdk.entities.request.head_node import HeadNode
from compute_sdk.entities.request.worker_group import WorkerGroup, Disk
from compute_sdk.service.deployment import Deployment


class TestDeployment(TestCase):

    def setUp(self):
        self.deployment = Deployment('stag')
        self.test_entity = ComputeDefinition(
            'sample', ['tag1'], 'runtime', 10, 'temp', HeadNode(2, 4),
            [WorkerGroup(2, 4, 2, 2, Disk('ssd', 320))],
            AdvanceConfig('dn1', 'dn2', 'dn3', 'dn4', 'dn5'))
        self.name = 'name-' + str(uuid.uuid4())

    """integration test"""

    def test_create_cluster(self):
        response = self.deployment.create_cluster(self.name, self.test_entity)
        print(response)
        resp_dict = json.loads(response)
        self.assertIn('ClusterName', resp_dict)
        self.assertIn('ArtifactS3Url', resp_dict)

    def test_update_cluster(self):
        response = self.deployment.update_cluster('name-33420f9a-3297-4a70-b02b-a118db0836cb', self.test_entity)
        print(response)
        resp_dict = json.loads(response)
        self.assertIn('ClusterName', resp_dict)
        self.assertIn('ArtifactS3Url', resp_dict)

    def test_start_cluster(self):
        response = self.deployment.start_cluster('name-33420f9a-3297-4a70-b02b-a118db0836cb')
        print(response)
        resp_dict = json.loads(response)
        self.assertIn('DashboardLink', resp_dict)
        self.assertIn('ClusterName', resp_dict)

    def test_restart_cluster(self):
        response = self.deployment.restart_cluster('name-33420f9a-3297-4a70-b02b-a118db0836cb')
        print(response)

    def test_stop_cluster(self):
        response = self.deployment.stop_cluster('name-33420f9a-3297-4a70-b02b-a118db0836cb')
        print(response)
        self.assertEqual(str(response), "\"cluster Stopped\"")

    def test_update_and_apply_cluster(self):
        response = self.deployment.update_cluster_and_apply('name-33420f9a-3297-4a70-b02b-a118db0836cb', self.test_entity)
        print(response)
        resp_dict = json.loads(response)
        self.assertIn('ClusterName', resp_dict)
        self.assertIn('DashboardLink', resp_dict)
