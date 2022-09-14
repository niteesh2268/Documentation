from typeguard import typechecked
from mlp_monitoring.constant.constants import CONFIGS_MAP


@typechecked
class Config:
    def __init__(self, env: str):
        self.env = env
        self._config = CONFIGS_MAP[self.env]

    @property
    def db_master_url(self):
        return self._config['mysql.master.url']

    @property
    def db_reader_url(self):
        return self._config['mysql.reader.url']

    @property
    def db_user(self):
        return self._config['username']

    @property
    def db_password(self):
        return self._config['password']

    @property
    def db_name(self):
        return self._config['database.name']
