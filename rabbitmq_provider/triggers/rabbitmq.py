from typing import Any, AsyncIterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.configuration import conf
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
import asyncio

class RabbitMQTriggers(BaseTrigger):
    def __init__(
        self,
        queue_name: str, 
        rabbitmq_conn_id: str = "rabbitmq_default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.poke_interval = poke_interval
        self._return_value = None

def serialize(self):
    return (
        "airflow.providers.rabbitmq.triggers.RabbitMQTriggers",
        {
            "queue_name": self.queue_name,
            "rabbitmq_conn_id": self.rabbitmq_conn_id,
            "poke_interval":self.poke_interval,
        }
    )

async def run(self) -> AsyncIterator[TriggerEvent]:
    """Asynchronously check for messages in RabbitMQ."""
    try:
        while True:
            if await self.poke(context={}):  # Call the asynchronous poke function
                yield TriggerEvent({"status": "running", "data": self._return_value})
            else:
                self.log.info("No message found in RabbitMQ")
            await asyncio.sleep(self.poke_interval)
    except Exception as e:
        yield TriggerEvent({"status": "error", "message": str(e)})


async def poke(self, context: dict):
    async with RabbitMQHook(self.rabbitmq_conn_id) as hook:
        message = await hook.pull(self.queue_name)
        if message is not None:
            self._return_value = message
            return True
        else:
            return False