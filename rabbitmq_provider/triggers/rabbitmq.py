from typing import AsyncIterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook
import asyncio


class RabbitMQTriggers(BaseTrigger):
    def __init__(
        self,
        queue_name: str,
        rabbitmq_conn_id: str = "rabbitmq_default",
        poke_interval: float = 5.0,
        context=None,
    ):
        super().__init__()
        self.queue_name = queue_name
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.poke_interval = poke_interval
        self.context = context
        self._return_value = None

    def serialize(self):
        return (
            "rabbitmq_provider.triggers.rabbitmq.RabbitMQTriggers",
            {
                "queue_name": self.queue_name,
                "rabbitmq_conn_id": self.rabbitmq_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Asynchronously check for messages in RabbitMQ."""
        try:
            while True:
                if await self.poke(self.context):  # Call the asynchronous poke function
                    yield TriggerEvent(
                        {"status": "running", "data": self._return_value}
                    )
                else:
                    self.log.info("No message found in RabbitMQ")
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

            
    async def poke(self, context: dict):
        try:
            async with RabbitMQHook(self.rabbitmq_conn_id) as hook:
                message = await hook.pull(self.queue_name)
                if message is not None:
                    self._return_value = message
                    return True
                else:
                    return False
        except Exception as e:
            self.log.exception("An error occurred in the poke method.")
            raise Exception(f"Error in poke method: {str(e)}")
