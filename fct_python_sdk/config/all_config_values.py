CONFIGS_MAP = {
    "local": {
        "datastore.configs": {
            "elastic-search.url": "http://localhost:9200",
            "elastic-search.id": "",
            "elastic-search.pass": "",
            "elastic-search-fct-index": "feature_group",
            "elastic-search-entity-index": "entity"
        },
        "fg.sink.s3.base_path": "s3://d11-data-lake/data-science/mlplatform/fg/"
    },
    "stag": {
        "datastore.configs": {
            "graph.db.url": "wss://mlplatform-netunedb-cluster.cluster-cfrymeo9e8jz.us-east-1.neptune.amazonaws.com:8182/gremlin",
            "elastic-search.url": "http://10.28.51.162:9200",
            "elastic-search.id": "",
            "elastic-search.pass": "",
            "elastic-search-fct-index": "feature_group",
            "elastic-search-entity-index": "entity"
        },
        "fg.sink.s3.base_path": "s3://d11-data-lake/data-science/mlplatform/fg/"
    },

    "test": {
        "datastore.configs": {
            "graph.db.url": "wss://mlplatform-netunedb-cluster.cluster-cfrymeo9e8jz.us-east-1.neptune.amazonaws.com:8182/gremlin",
            "elastic-search.id": "",
            "elastic-search.pass": "",
            "elastic-search-fct-index": "feature_group",
            "elastic-search-entity-index": "entity"
        }
    },

    "prod": {
        "datastore.configs": {
            "graph.db.url": "wss://mlplatform-netunedb.cluster-cefqnpdgdxcd.us-east-1.neptune.amazonaws.com:8182/gremlin",
            "elastic-search.url": "https://es-elkmlplat-kibana.d11tech.in:443",
            "elastic-search.id": "elastic",
            "elastic-search.pass": "elk@12345",
            "elastic-search-fct-index": "feature_group",
            "elastic-search-entity-index": "entity"
        }
    }
}
