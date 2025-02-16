from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import timedelta
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
from rabbitmq_provider.triggers.rabbitmq import RabbitMQTriggers
from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
)
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
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.deferrable = deferrable
        self._return_value = None

    def execute(self, context: dict):
        """Overridden to allow messages to be passed"""
        self.log.info("--- Inside execute method ----")
        if not self.deferrable:
            super().execute(context)
            return self._return_value
        else:
            if not self.poke(context=context):
                self._defer(context)
            else:
                super().execute(context)
                return self._return_value

    def _defer(self, context: dict) -> None:
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=RabbitMQTriggers(
                queue_name=self.queue_name,
                rabbitmq_conn_id=self.rabbitmq_conn_id,
                poke_interval=self.poke_interval,
                context=context,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: dict, event: Union[Dict[str, Any], None] = None
    ) -> None:
        """
        Execute when a message is received from RabbitMQ.
        Relies on event data to determine success or error.
        """
        if event["status"] == "running":
            data = event.get("data", None)
            if data:
                self.log.info("Successfully processed message from RabbitMQ: %s", data)
            else:
                self.log.error("Received empty message from RabbitMQ")
        elif event["status"] == "error":
            if self.soft_fail:
                raise AirflowSkipException(event["message"])
            raise AirflowException(event["message"])

    def poke(self, context: dict):
        hook = RabbitMQHook(self.rabbitmq_conn_id)
        message = hook.pull(self.queue_name)
        if message is not None:
            self._return_value = message
            return True
        else:
            return False
