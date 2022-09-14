from compute_sdk.entities.response.entity import InstanceRole, Azs, WorkerTemplate

CONFIGS_MAP = {
    'stag': {
        'elasticsearch.url': 'http://10.150.102.51:9200',
        'compute.url': 'http://ip-10-28-166-7.ec2.internal:8080/',
    },
    'prod': {
        'elasticsearch.url': 'http://0.0.0.0:9200',
        'compute.url': 'http://0.0.0.0/',
    },
    'local': {
        'elasticsearch.url': 'http://0.0.0.0:9200',
        'compute.url': 'http://0.0.0.0/',
    }
}

VALUES_YAML_FILE_PATH = 'compute_sdk/resources/values.yaml'
ALREADY_EXIST = "Cluster with same name already exist"
SUCCESS = "SUCCESS"
UPDATE = "UPDATE"
ERROR = "ERROR"
AZS = [Azs("az_id", "az_name").to_dict()]
INSTANCE_ROLE = [InstanceRole("template_id", "template_name").to_dict()]
RUNTIMES = ["dummy1", "dummy2"]
TEMPLATES = [WorkerTemplate(1, "dummy_template_name", 10).to_dict()]
DISK_TYPE = ["disk_type1"]
TAGS_FIELD = "tags"
INDEX = 'computea'

CLUSTER_START_URL = 'compute/v1/cluster/start'
CLUSTER_STOP_URL = 'compute/v1/cluster/stop'
CLUSTER_RESTART_URL = 'compute/v1/cluster/restart'
CLUSTER_CREATE_URL = 'compute/v1/cluster'

ELASTIC_INDEX_MAPPING = {"mappings": {"properties": {"advance_config": {"properties": {
    "availability_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "env_variables": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "init_script": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "instance_role": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "log_path": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "head_node": {
    "properties": {"head_node_cores": {"type": "long"}, "head_node_memory": {"type": "long"}}},
    "infra_template": {"type": "text", "fields": {
        "keyword": {"type": "keyword", "ignore_above": 256}}},
    "name": {"type": "keyword", "fields": {
        "keyword": {"type": "keyword", "ignore_above": 256}}},
    "runtime": {"type": "text", "fields": {
        "keyword": {"type": "keyword", "ignore_above": 256}}},
    "tags": {"type": "text", "fields": {
        "keyword": {"type": "keyword", "ignore_above": 256}}},
    "terminate_after_minutes": {"type": "long"}, "worker_group": {
        "properties": {"cores": {"type": "long"}, "disk_size": {"type": "long"},
                       "disk_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                       "max_pods": {"type": "long"}, "memory": {"type": "long"}, "min_pods": {"type": "long"}}}}}}
