from fct_python_sdk.config.all_config_values import CONFIGS_MAP


class Config(object):
    def __init__(self, env):
        self.env = env
        self._config = CONFIGS_MAP[self.env]
        print("loaded the below configs for the environement of " + env)
        print(self._config)

    @property
    def graph_db_url(self):
        return self._config['datastore.configs']['graph.db.url']
