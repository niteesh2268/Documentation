from enum import Enum


class Status(Enum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    CREATING = 'creating'


CONFIGS_MAP = {
    'prod': {
        'mysql.master.url': 'localhost',
        'mysql.reader.url': 'localhost',
        'username': '',
        'password': '',
        'database.name': ''
    },
    'stag': {
        'mysql.master.url': 'mlp-monitoring-rds-master-mlp-dev-1.dream11-load.local',
        'mysql.reader.url': 'mlp-monitoring-rds-reader-mlp-dev-1.dream11-load.local',
        'username': 'd11stag',
        'password': 'ENQmzHfQX4XR',
        'database.name': 'mlp_monitoring'
    },
    'dev': {
        'mysql.master.url': 'localhost',
        'mysql.reader.url': 'localhost',
        'username': 'root',
        'password': '',
        'database.name': 'mlp_monitoring'
    }
}

SUCCESS = "SUCCESS"
ERROR = "ERROR"
