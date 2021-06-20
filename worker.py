import os

from celery import Celery


celery_worker = Celery(__name__, include=['tasks'])
celery_worker.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_worker.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")


if __name__ == "__main__":
    celery_worker.start()
