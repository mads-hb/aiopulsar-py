import asyncio
import pulsar
from functools import partial
from typing import Union, Iterable, Optional
import concurrent.futures


class Producer:
    def __init__(
        self,
        *,
        executor: Optional[concurrent.futures.Executor] = None,
        loop: Optional[asyncio.BaseEventLoop] = None,
        producer=pulsar.Producer,
    ):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._producer = producer

    def _execute(self, func, *args, **kwargs):
        # execute function with args and kwargs in executor.
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    def topic(self) -> Union[str, Iterable[str]]:
        """
        Return the topic which producer is publishing to
        """
        return self._producer.topic()

    def producer_name(self) -> str:
        """
        Return the producer name which could have been assigned by the
        system or specified by the client
        """
        return self._producer.producer_name()

    def last_sequence_id(self) -> pulsar.MessageId:
        """
        Get the last sequence id that was published by this producer.
        This represent either the automatically assigned or custom sequence id
        (set on the `MessageBuilder`) that was published and acknowledged by the broker.
        After recreating a producer with the same producer name, this will return the
        last message that was published in the previous producer session, or -1 if
        there no message was ever published.
        """
        return self._producer.last_sequence_id()

    async def send(
        self,
        content,
        properties=None,
        partition_key=None,
        sequence_id=None,
        replication_clusters=None,
        disable_replication=False,
        event_timestamp=None,
        deliver_at=None,
        deliver_after=None,
    ) -> None:
        """
        Publish a message on the topic. Blocks until the message is acknowledged
        Returns a `MessageId` object that represents where the message is persisted.
        **Args**
        * `content`:
          A `bytes` object with the message payload.
        **Options**
        * `properties`:
          A dict of application-defined string properties.
        * `partition_key`:
          Sets the partition key for message routing. A hash of this key is used
          to determine the message's topic partition.
        * `sequence_id`:
          Specify a custom sequence id for the message being published.
        * `replication_clusters`:
          Override namespace replication clusters. Note that it is the caller's
          responsibility to provide valid cluster names and that all clusters
          have been previously configured as topics. Given an empty list,
          the message will replicate according to the namespace configuration.
        * `disable_replication`:
          Do not replicate this message.
        * `event_timestamp`:
          Timestamp in millis of the timestamp of event creation
        * `deliver_at`:
          Specify the this message should not be delivered earlier than the
          specified timestamp.
          The timestamp is milliseconds and based on UTC
        * `deliver_after`:
          Specify a delay in timedelta for the delivery of the messages.
        """
        fut = self._execute(
            self._producer.send,
            content,
            properties=properties,
            partition_key=partition_key,
            sequence_id=sequence_id,
            replication_clusters=replication_clusters,
            disable_replication=disable_replication,
            event_timestamp=event_timestamp,
            deliver_at=deliver_at,
            deliver_after=deliver_after,
        )
        await fut

    async def flush(self) -> None:
        """
        Flush all the messages buffered in the client and wait until all messages have been
        successfully persisted
        """
        fut = self._execute(self._producer.flush)
        await fut

    async def close(self) -> None:
        """
        Close the producer.
        """
        fut = self._execute(self._producer.close)
        await fut
        self._producer = None

    def is_connected(self) -> bool:
        return self._producer.is_connected()
