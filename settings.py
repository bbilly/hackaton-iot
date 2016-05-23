CELERY_BROKER_URL = 'sqla+sqlite:///rest.db'
CELERY_RESULT_BACKEND = 'sqla+sqlite:///rest.db'
DATABASE = 'rest.db'
NB_MSG_TO_COMMIT = 10000
BROKER_POOL_LIMIT = 1000
