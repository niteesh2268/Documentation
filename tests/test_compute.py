from unittest import TestCase

from compute_sdk.compute import Compute
from compute_sdk.constant.constants import SUCCESS, UPDATE
from compute_sdk.entities.request.advance_config import AdvanceConfig
from compute_sdk.entities.request.compute_request import ComputeDefinition
from compute_sdk.entities.request.head_node import HeadNode
from compute_sdk.entities.request.worker_group import WorkerGroup, Disk


class TestCompute(TestCase):
    def setUp(self):
        self.compute = Compute('local')
        self.test_entity = ComputeDefinition(
            'sample', ['tag1'], 'runtime', 10, 'temp', HeadNode(8, 9),
            [WorkerGroup(4, 32, 1, 10, Disk('ssd', 320)),
             WorkerGroup(1, 2, 3, 4, Disk('hdd', 250))],
            AdvanceConfig('dn1', 'dn2', 'dn3', 'dn4', 'dn5'))
        self.test_entity2 = ComputeDefinition(
            'sample', ['tag1'], 'runtime', 11, 'temp', HeadNode(1, 7),
            [WorkerGroup(4, 32, 1, 10, Disk('ssd', 320))],
            AdvanceConfig('dn1', 'dn2', 'dn3', 'dn4', 'dn5'))
        self.file_path = "request.yaml"

    def test_create_cluster_with_yaml(self):
        resp = self.compute.create_cluster_with_yaml(self.file_path)
        print(resp)
        self.assertEqual(SUCCESS, resp.status)

    def test_create_cluster(self):
        resp = self.compute.create_cluster(self.test_entity)
        print(resp)
        self.assertEqual(SUCCESS, resp.status)

    def test_delete_cluster(self):
        resp = self.compute.delete_cluster("9yleEoMBNTZI1aQmIVQk")
        print(resp)
        self.assertEqual(SUCCESS, resp.status)

    def test_get_cluster(self):
        resp = self.compute.get_cluster("-ilvEoMBNTZI1aQmslTx")
        print(resp)
        self.assertEqual(SUCCESS, resp.status)

    def test_update_cluster(self):
        resp = self.compute.update_and_restart_cluster_details("9yleEoMBNTZI1aQmIVQk", self.test_entity)
        print(resp)
        self.assertEqual(SUCCESS, resp.status)
        self.assertEqual(UPDATE, resp.operation)
        self.assertEqual(SUCCESS, resp.status)

    def test_update_cluster_name(self):
        resp = self.compute.update_cluster_name("9yleEoMBNTZI1aQmIVQk", "new_name")
        print(resp)
        self.assertEqual(SUCCESS, resp.status)
        self.assertEqual(UPDATE, resp.operation)
        self.assertEqual(SUCCESS, resp.status)

    def test_update_cluster_tags(self):
        resp = self.compute.update_cluster_tags("9yleEoMBNTZI1aQmIVQk", ["new_tag_list"])
        print(resp)
        self.assertEqual(SUCCESS, resp.status)
        self.assertEqual(UPDATE, resp.operation)
        self.assertEqual(SUCCESS, resp.status)

    def test_update_and_restart_cluster_details(self):
        resp = self.compute.update_and_restart_cluster_details("9yleEoMBNTZI1aQmIVQk", self.test_entity)
        print(resp)
        self.assertEqual(SUCCESS, resp.status)
        self.assertEqual(UPDATE, resp.operation)
        self.assertEqual(SUCCESS, resp.status)

    def test_search_cluster(self):
        resp = self.compute.search_cluster("sample", "filter", 0, 10, "name", "desc")
        print(resp)
        self.assertEqual(SUCCESS, resp.status)

    def test_get_runtimes(self):
        resp = self.compute.get_runtimes()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual(['dummy1', 'dummy2'], resp["data"])

    def test_get_templates(self):
        resp = self.compute.get_templates()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual([{'display_name': 'dummy_template_name', 'memory_per_core': 10, 'template_id': 1}],
                         resp["data"])

    def test_get_disk_types(self):
        resp = self.compute.get_disk_types()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual(['disk_type1'], resp["data"])

    def test_get_instance_role(self):
        resp = self.compute.get_instance_role()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual([{'display_name': 'template_name', 'instance_role_id': 'template_id'}], resp["data"])

    def test_get_az(self):
        resp = self.compute.get_az()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual([{'az_id': 'az_id', 'display_name': 'az_name'}], resp["data"])

    def test_get_tags(self):
        resp = self.compute.get_tags()
        print(resp)
        self.assertEqual(SUCCESS, resp["status"])
        self.assertEqual(['tag1', 'MVP', 'new_tag_list', 'test'], resp["data"])

    def test_search_cluster_name(self):
        resp = self.compute.search_cluster_name("sample")
        print(resp)
        self.assertEqual(SUCCESS, resp.status)
