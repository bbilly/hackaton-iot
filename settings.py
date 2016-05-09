#CELERY_BROKER_URL='sqla+sqlite:///rest.db'
#CELERY_RESULT_BACKEND='sqla+sqlite:///rest.db'
BROKER_URL = 'redis://localhost:6379/0'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

