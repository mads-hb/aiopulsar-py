import asyncio
import pulsar
from functools import partial
from typing import Union, Iterable, Optional
import concurrent.futures


class Consumer:
    def __init__(
        self,
        *,
        executor: Optional[concurrent.futures.Executor] = None,
        loop: Optional[asyncio.BaseEventLoop] = None,
        consumer=pulsar.Consumer,
    ):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._consumer = consumer

    def _execute(self, func, *args, **kwargs):
        # execute function with args and kwargs in executor.
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    def topic(self) -> Union[str, Iterable[str]]:
        """
        Return the topic this consumer is subscribed to.
        """
        return self._consumer.topic()

    def subscription_name(self) -> str:
        """
        Return the subscription name.
        """
        return self._consumer.subscription_name()

    async def unsubscribe(self) -> None:
        """
        Unsubscribe the current consumer from the topic.
        This method will block until the operation is completed. Once the
        consumer is unsubscribed, no more messages will be received and
        subsequent new messages will not be retained for this consumer.
        This consumer object cannot be reused.
        """
        fut = self._execute(self._consumer.unsubscribe)
        await fut

    async def receive(self, timeout_millis: Optional[int] = None) -> pulsar.Message:
        """
        Receive a single message.
        If a message is not immediately available, this method will block until
        a new message is available.
        **Options**
        * `timeout_millis`:
          If specified, the receive will raise an exception if a message is not
          available within the timeout.
        """
        fut = self._execute(self._consumer.receive, timeout_millis)
        result = await fut
        return result

    async def acknowledge(
        self, message: Union[pulsar.Message, pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of a single message.
        This method will block until an acknowledgement is sent to the broker.
        After that, the message will not be re-delivered to this consumer.
        **Args**
        * `message`:
          The received message or message id.
        """
        fut = self._execute(self._consumer.acknowledge, message)
        await fut

    async def acknowledge_cumulative(
        self, message: Union[pulsar.Message, pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the reception of all the messages in the stream up to (and
        including) the provided message.
        This method will block until an acknowledgement is sent to the broker.
        After that, the messages will not be re-delivered to this consumer.
        **Args**
        * `message`:
          The received message or message id.
        """
        fut = self._execute(self._consumer.acknowledge_cumulative, message)
        await fut

    async def negative_acknowledge(
        self, message: Union[pulsar.Message, pulsar.MessageId]
    ) -> None:
        """
        Acknowledge the failure to process a single message.
        When a message is "negatively acked" it will be marked for redelivery after
        some fixed delay. The delay is configurable when constructing the consumer
        with {@link ConsumerConfiguration#setNegativeAckRedeliveryDelayMs}.
        This call is not blocking.
        **Args**
        * `message`:
          The received message or message id.
        """
        fut = self._execute(self._consumer.negative_acknowledge, message)
        await fut

    def pause_message_listener(self) -> None:
        """
        Pause receiving messages via the `message_listener` until
        `resume_message_listener()` is called.
        """
        self._consumer.pause_message_listener()

    def resume_message_listener(self):
        """
        Resume receiving the messages via the message listener.
        Asynchronously receive all the messages enqueued from the time
        `pause_message_listener()` was called.
        """
        self._consumer.resume_message_listener()

    def redeliver_unacknowledged_messages(self):
        """
        Redelivers all the unacknowledged messages. In failover mode, the
        request is ignored if the consumer is not active for the given topic. In
        shared mode, the consumer's messages to be redelivered are distributed
        across all the connected consumers. This is a non-blocking call and
        doesn't throw an exception. In case the connection breaks, the messages
        are redelivered after reconnect.
        """
        self._consumer.redeliver_unacknowledged_messages()

    async def seek(self, messageid: Union[int, pulsar.MessageId]) -> None:
        """
        Reset the subscription associated with this consumer to a specific message id or publish timestamp.
        The message id can either be a specific message or represent the first or last messages in the topic.
        Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
        seek() on the individual partitions.
        **Args**
        * `message`:
          The message id for seek, OR an integer event time to seek to
        """
        fut = self._execute(self._consumer.seek, messageid)
        await fut

    async def close(self) -> None:
        """
        Close the consumer.
        """
        fut = self._execute(self._consumer.close)
        await fut
        self._consumer = None

    def is_connected(self) -> bool:
        """
        Check if the consumer is connected or not.
        """
        return self._consumer.is_connected()
