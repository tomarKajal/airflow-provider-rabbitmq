import logging
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.triggers.temporal import TimeDeltaTrigger
from datetime import timedelta, datetime
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
from airflow.configuration import conf
from typing import Any, Dict, Union

class RabbitMQSensor(BaseSensorOperator):
    """RabbitMQ sensor that monitors a queue for any messages.

    :param queue_name: The name of the queue to monitor
    :type queue_name: str
    :param rabbitmq_conn_id: connection that has the RabbitMQ
    connection (i.e amqp://guest:guest@localhost:5672), defaults to "rabbitmq_default"
    :type rabbitmq_conn_id: str, optional
    """

    template_fields = ["queue_name"]
    ui_color = "#ff6600"

    @apply_defaults
    def __init__(
        self,
        queue_name: str, 
        rabbitmq_conn_id: str = "rabbitmq_default", 
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.deferrable = deferrable
        self._return_value = None

    def execute(self, context: dict):
        """Overridden to allow messages to be passed"""
        logging.info("--- Inside execute method ----")
        if not self.deferrable:
            super().execute(context)
            return self._return_value
        else:
            if not self.poke(context=context):
                self._defer()

    def _defer(self) -> None:
        self.defer(
            trigger=TimeDeltaTrigger(delta=timedelta(minutes=5)),
            method_name="execute_complete",
        )

    '''def execute_complete(self, context: dict):
        super().execute(context)
        return self._return_value'''
    
    def execute_complete(self,context: dict,event: Union[Dict[str, Any], None] = None) -> None:
        logging.info("--- Inside execute complete ----")
        logging.info(f"printing event: {event}")
        return

    def poke(self, context: dict):
        hook = RabbitMQHook(self.rabbitmq_conn_id)
        message = hook.pull(self.queue_name)
        if message is not None:
            self._return_value = message
            return True
        else:
            return False
