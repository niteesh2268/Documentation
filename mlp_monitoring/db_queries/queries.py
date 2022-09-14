GET_ALL_CLUSTERS_STATUS = """
SELECT * FROM cluster_status
"""

INSERT_CLUSTER_STATUS = """
INSERT INTO cluster_status(cluster_id, artifact_id, status, cluster_name) 
VALUES (%s, %s, %s, %s)
"""

UPDATE_CLUSTER = """
UPDATE cluster_status 
SET status='%s', active_pods='%s', available_memory='%s', active_cluster_runid='%s', artifact_id='%s' 
WHERE cluster_id='%s'
"""

UPDATE_CLUSTER_STATUS = """
UPDATE cluster_status 
SET status='%s'
WHERE cluster_id='%s'
"""

UPDATE_CLUSTER_ARTIFACT = """
UPDATE cluster_status 
SET artifact_id='%s' 
WHERE cluster_id='%s'
"""

UPDATE_CLUSTER_URLS = """
UPDATE cluster_status
SET dashboard_link='%s', notebook_link='%s'
WHERE cluster_id='%s'
"""

UPDATE_CLUSTER_NAME = """
UPDATE cluster_status
SET cluster_name='%s'
WHERE cluster_id='%s'
"""

DELETE_CLUSTER = """
DELETE FROM cluster_status where cluster_id='%s'
"""

GET_CLUSTER_LAST_UPDATED = """
SELECT * 
FROM cluster_status
WHERE cluster_id='%s'
"""

INSERT_CLUSTER_ACTION = """
INSERT INTO cluster_actions(cluster_runid, action, message, cluster_id, artifact_id)
VALUES (%s, %s, %s, %s, %s)
"""

UPDATE_CLUSTER_RUN_ID = """
UPDATE cluster_status
SET active_cluster_runid='%s'
WHERE cluster_id='%s'
"""

GET_CLUSTERS_FROM_LIST = """
SELECT *
FROM cluster_status
WHERE cluster_id IN 
"""

DELETE_CLUSTER_ACTIONS = """
DELETE FROM cluster_actions
where cluster_id='%s'
"""
