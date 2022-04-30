import asyncio
import pulsar
from functools import partial
from typing import Union, Iterable, Optional
import concurrent.futures


class Reader:
    def __init__(
        self,
        *,
        executor: Optional[concurrent.futures.Executor] = None,
        loop: Optional[asyncio.BaseEventLoop] = None,
        reader=pulsar.Reader,
    ):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._reader = reader

    def _execute(self, func, *args, **kwargs):
        # execute function with args and kwargs in executor.
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    def topic(self) -> Union[str, Iterable[str]]:
        """
        Return the topic this reader is reading from.
        """
        return self._reader.topic()

    async def read_next(self) -> pulsar.Message:
        """
        Read a single message.
        If a message is not immediately available, this method will block until
        a new message is available.
        **Options**
        """
        fut = self._execute(self._reader.read_next)
        result = await fut
        return result

    def has_message_available(self) -> bool:
        """
        Check if there is any message available to read from the current position.
        """
        return self._reader.has_message_available()

    async def seek(self, messageid: Union[int, pulsar.MessageId]) -> None:
        """
        Reset this reader to a specific message id or publish timestamp.
        The message id can either be a specific message or represent the first or last messages in the topic.
        Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
        seek() on the individual partitions.
        **Args**
        * `message`:
          The message id for seek, OR an integer event time to seek to
        """
        fut = self._execute(self._reader.seek, messageid)
        await fut

    async def close(self) -> None:
        """
        Close the reader.
        """
        await self._execute(self._reader.close)
        self._reader = None

    def is_connected(self) -> bool:
        """
        Check if the reader is connected or not.
        """
        return self._reader.is_connected()
