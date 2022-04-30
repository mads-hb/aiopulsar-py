import asyncio
import concurrent.futures
import logging
import os
from functools import partial
from typing import Optional, Union, AsyncContextManager
import pulsar
from aiopulsar.utils import (
    _ClientContextManager,
    _ReaderContextManager,
    _ConsumerContextManager,
    _ProducerContextManager,
)
from aiopulsar.producer import Producer
from aiopulsar.consumer import Consumer
from aiopulsar.reader import Reader


def connect(
    service_url: str,
    authentication: Optional[pulsar.Authentication] = None,
    operation_timeout_seconds: int = 30,
    io_threads: int = 1,
    message_listener_threads: int = 1,
    concurrent_lookup_requests: int = 50000,
    log_conf_file_path: Optional[Union[os.PathLike, str]] = None,
    use_tls: bool = False,
    tls_trust_certs_file_path: Optional[Union[os.PathLike, str]] = None,
    tls_allow_insecure_connection: bool = False,
    tls_validate_hostname: bool = False,
    logger: Optional[logging.Logger] = None,
    connection_timeout_ms: int = 10000,
    listener_name: Optional[str] = None,
) -> _ClientContextManager:
    coro = _connect(
        service_url,
        authentication=authentication,
        operation_timeout_seconds=operation_timeout_seconds,
        io_threads=io_threads,
        message_listener_threads=message_listener_threads,
        concurrent_lookup_requests=concurrent_lookup_requests,
        log_conf_file_path=log_conf_file_path,
        use_tls=use_tls,
        tls_trust_certs_file_path=tls_trust_certs_file_path,
        tls_allow_insecure_connection=tls_allow_insecure_connection,
        tls_validate_hostname=tls_validate_hostname,
        logger=logger,
        connection_timeout_ms=connection_timeout_ms,
        listener_name=listener_name,
    )
    return _ClientContextManager(coro)


async def _connect(*args, **kwargs) -> "Client":
    client = Client(*args, **kwargs)
    await client._connect()
    return client


class Client:
    def __init__(
        self,
        service_url: str,
        *,
        executor: Optional[concurrent.futures.Executor] = None,
        loop: Optional[asyncio.BaseEventLoop] = None,
        **kwargs
    ):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._client = None

        self._service_url = service_url
        self._kwargs = kwargs

    def _execute(self, func, *args, **kwargs):
        # execute function with args and kwargs in executor.
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    async def _connect(self):
        # create pyodbc connection
        f = self._execute(pulsar.Client, **self._kwargs)
        self._client = await f

    @property
    def loop(self):
        return self._loop

    @property
    def closed(self):
        if self._client:
            return False
        return True

    async def close(self):
        """Close pulsar client connection"""
        fut = self._execute(self._client.close)
        await fut
        self._client = None

    async def shutdown(self):
        self._client.shutdown()
        await self.close()

    async def create_producer(self, *args, **kwargs) -> AsyncContextManager[Producer]:
        if self._client:
            fut = self._execute(self._client.create_producer, *args, **kwargs)
            return _ProducerContextManager(fut)
        else:
            raise ValueError("Client is closed.")

    async def subscribe(self, *args, **kwargs) -> AsyncContextManager[Consumer]:
        if self._client:
            fut = self._execute(self._client.subscribe, *args, **kwargs)
            return _ConsumerContextManager(fut)
        else:
            raise ValueError("Client is closed.")

    async def create_reader(self, *args, **kwargs) -> AsyncContextManager[Reader]:
        if self._client:
            fut = self._execute(self._client.create_reader, *args, **kwargs)
            return _ReaderContextManager(fut)
        else:
            raise ValueError("Client is closed.")
