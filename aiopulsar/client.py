import asyncio
import concurrent.futures
import logging
import os
from functools import partial
from typing import List, Optional, Union, AsyncContextManager
import pulsar
from aiopulsar.utils import _ClientContextManager, _ContextManager
from aiopulsar.producer import Producer
from aiopulsar.consumer import Consumer
from aiopulsar.reader import Reader


def connect(
    service_url: str,
    executor: Optional[concurrent.futures.Executor] = None,
    loop: Optional[asyncio.BaseEventLoop] = None,
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
) -> _ClientContextManager:
    """This function is used to create new Client connections. The client connection is returned as an asynchronous
    context manager.

    **Args**
    * `service_url`: The Pulsar service url eg: pulsar://my-broker.com:6650/

    **Options**
    * `executor`:
        Executor to use for synchronous tasks.
    * `loop`:
        Event loop to use.
    * `authentication`:
      Set the authentication provider to be used with the broker. For example:
      `AuthenticationTls`, `AuthenticationToken`, `AuthenticationAthenz` or `AuthenticationOauth2`
    * `operation_timeout_seconds`:
      Set timeout on client operations (subscribe, create producer, close,
      unsubscribe).
    * `io_threads`:
      Set the number of IO threads to be used by the Pulsar client.
    * `message_listener_threads`:
      Set the number of threads to be used by the Pulsar client when
      delivering messages through message listener. The default is 1 thread
      per Pulsar client. If using more than 1 thread, messages for distinct
      `message_listener`s will be delivered in different threads, however a
      single `MessageListener` will always be assigned to the same thread.
    * `concurrent_lookup_requests`:
      Number of concurrent lookup-requests allowed on each broker connection
      to prevent overload on the broker.
    * `log_conf_file_path`:
      Initialize log4cxx from a configuration file.
    * `use_tls`:
      Configure whether to use TLS encryption on the connection. This setting
      is deprecated. TLS will be automatically enabled if the `serviceUrl` is
      set to `pulsar+ssl://` or `https://`
    * `tls_trust_certs_file_path`:
      Set the path to the trusted TLS certificate file. If empty defaults to
      certifi.
    * `tls_allow_insecure_connection`:
      Configure whether the Pulsar client accepts untrusted TLS certificates
      from the broker.
    * `tls_validate_hostname`:
      Configure whether the Pulsar client validates that the hostname of the
      endpoint, matches the common name on the TLS certificate presented by
      the endpoint.
    * `logger`:
      Set a Python logger for this Pulsar client. Should be an instance of `logging.Logger`.
    * `connection_timeout_ms`:
      Set timeout in milliseconds on TCP connections.
    """
    coro = _connect(
        service_url,
        loop=loop,
        executor=executor,
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
        """Initialize a Client class."""
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
        # create client connection
        f = self._execute(pulsar.Client, self._service_url, **self._kwargs)
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

    async def get_topic_partitions(self, topic) -> List[str]:
        """Get the list of partitions for a given topic.
        If the topic is partitioned, this will return a list of partition names. If the topic is not
        partitioned, the returned list will contain the topic name itself.
        This can be used to discover the partitions and create Reader, Consumer or Producer
        instances directly on a particular partition.
        :param topic: the topic name to lookup
        :return: a list of partition name
        """
        if self._client:
            fut = self._execute(self._client.get_topic_partitions, topic)
            partitions = await fut
            return partitions
        else:
            raise ValueError("Cleint is closed.")

    async def _reader(self, *args, **kwargs) -> Reader:
        if self._client:
            reader = await self._execute(self._client.create_reader, *args, **kwargs)
            return Reader(executor=self._executor, loop=self._loop, reader=reader)
        else:
            raise ValueError("Client is closed.")

    def create_reader(self, *args, **kwargs) -> AsyncContextManager[Reader]:
        """
        Create a reader on a particular topic
        **Args**
        * `topic`: The name of the topic.
        * `start_message_id`: The initial reader positioning is done by specifying a message id.
           The options are:
            * `MessageId.earliest`: Start reading from the earliest message available in the topic
            * `MessageId.latest`: Start reading from the end topic, only getting messages published
               after the reader was created
            * `MessageId`: When passing a particular message id, the reader will position itself on
               that specific position. The first message to be read will be the message next to the
               specified messageId. Message id can be serialized into a string and deserialized
               back into a `MessageId` object:
                   # Serialize to string
                   s = msg.message_id().serialize()
                   # Deserialize from string
                   msg_id = MessageId.deserialize(s)
        **Options**
        * `schema`:
           Define the schema of the data that will be received by this reader.
        * `reader_listener`:
          Sets a message listener for the reader. When the listener is set,
          the application will receive messages through it. Calls to
          `reader.read_next()` will not be allowed. The listener function needs
          to accept (reader, message), for example:
                def my_listener(reader, message):
                    # process message
                    pass
        * `receiver_queue_size`:
          Sets the size of the reader receive queue. The reader receive
          queue controls how many messages can be accumulated by the reader
          before the application calls `read_next()`. Using a higher value could
          potentially increase the reader throughput at the expense of higher
          memory utilization.
        * `reader_name`:
          Sets the reader name.
        * `subscription_role_prefix`:
          Sets the subscription role prefix.
        * `is_read_compacted`:
          Selects whether to read the compacted version of the topic
        * crypto_key_reader:
           Symmetric encryption class implementation, configuring public key encryption messages for the producer
           and private key decryption messages for the consumer
        """
        coro = self._reader(*args, **kwargs)
        return _ContextManager(coro)

    async def _consumer(self, *args, **kwargs) -> Consumer:
        if self._client:
            consumer = await self._execute(self._client.subscribe, *args, **kwargs)
            return Consumer(executor=self._executor, loop=self._loop, consumer=consumer)
        else:
            raise ValueError("Client is closed.")

    def subscribe(self, *args, **kwargs) -> AsyncContextManager[Consumer]:
        """
        Subscribe to the given topic and subscription combination.
        **Args**
        * `topic`: The name of the topic, list of topics or regex pattern.
                  This method will accept these forms:
                    - `topic='my-topic'`
                    - `topic=['topic-1', 'topic-2', 'topic-3']`
                    - `topic=re.compile('persistent://public/default/topic-*')`
        * `subscription`: The name of the subscription.
        **Options**
        * `consumer_type`:
          Select the subscription type to be used when subscribing to the topic.
        * `schema`:
           Define the schema of the data that will be received by this consumer.
        * `message_listener`:
          Sets a message listener for the consumer. When the listener is set,
          the application will receive messages through it. Calls to
          `consumer.receive()` will not be allowed. The listener function needs
          to accept (consumer, message), for example:
                #!python
                def my_listener(consumer, message):
                    # process message
                    consumer.acknowledge(message)
        * `receiver_queue_size`:
          Sets the size of the consumer receive queue. The consumer receive
          queue controls how many messages can be accumulated by the consumer
          before the application calls `receive()`. Using a higher value could
          potentially increase the consumer throughput at the expense of higher
          memory utilization. Setting the consumer queue size to zero decreases
          the throughput of the consumer by disabling pre-fetching of messages.
          This approach improves the message distribution on shared subscription
          by pushing messages only to those consumers that are ready to process
          them. Neither receive with timeout nor partitioned topics can be used
          if the consumer queue size is zero. The `receive()` function call
          should not be interrupted when the consumer queue size is zero. The
          default value is 1000 messages and should work well for most use
          cases.
        * `max_total_receiver_queue_size_across_partitions`
          Set the max total receiver queue size across partitions.
          This setting will be used to reduce the receiver queue size for individual partitions
        * `consumer_name`:
          Sets the consumer name.
        * `unacked_messages_timeout_ms`:
          Sets the timeout in milliseconds for unacknowledged messages. The
          timeout needs to be greater than 10 seconds. An exception is thrown if
          the given value is less than 10 seconds. If a successful
          acknowledgement is not sent within the timeout, all the unacknowledged
          messages are redelivered.
        * `negative_ack_redelivery_delay_ms`:
           The delay after which to redeliver the messages that failed to be
           processed (with the `consumer.negative_acknowledge()`)
        * `broker_consumer_stats_cache_time_ms`:
          Sets the time duration for which the broker-side consumer stats will
          be cached in the client.
        * `is_read_compacted`:
          Selects whether to read the compacted version of the topic
        * `properties`:
          Sets the properties for the consumer. The properties associated with a consumer
          can be used for identify a consumer at broker side.
        * `pattern_auto_discovery_period`:
          Periods of seconds for consumer to auto discover match topics.
        * `initial_position`:
          Set the initial position of a consumer  when subscribing to the topic.
          It could be either: `InitialPosition.Earliest` or `InitialPosition.Latest`.
          Default: `Latest`.
        * crypto_key_reader:
           Symmetric encryption class implementation, configuring public key encryption messages for the producer
           and private key decryption messages for the consumer
        * replicate_subscription_state_enabled:
          Set whether the subscription status should be replicated.
          Default: `False`.
        """
        coro = self._consumer(*args, **kwargs)
        return _ContextManager(coro)

    async def _producer(self, *args, **kwargs) -> Producer:
        if self._client:
            producer = await self._execute(
                self._client.create_producer, *args, **kwargs
            )
            return Producer(executor=self._executor, loop=self._loop, producer=producer)
        else:
            raise ValueError("Client is closed.")

    def create_producer(self, *args, **kwargs) -> AsyncContextManager[Producer]:
        """
        Create a new producer on a given topic.
        **Args**
        * `topic`:
          The topic name
        **Options**
        * `producer_name`:
           Specify a name for the producer. If not assigned,
           the system will generate a globally unique name which can be accessed
           with `Producer.producer_name()`. When specifying a name, it is app to
           the user to ensure that, for a given topic, the producer name is unique
           across all Pulsar's clusters.
        * `schema`:
           Define the schema of the data that will be published by this producer.
           The schema will be used for two purposes:
             - Validate the data format against the topic defined schema
             - Perform serialization/deserialization between data and objects
           An example for this parameter would be to pass `schema=JsonSchema(MyRecordClass)`.
        * `initial_sequence_id`:
           Set the baseline for the sequence ids for messages
           published by the producer. First message will be using
           `(initialSequenceId + 1)`` as its sequence id and subsequent messages will
           be assigned incremental sequence ids, if not otherwise specified.
        * `send_timeout_millis`:
          If a message is not acknowledged by the server before the
          `send_timeout` expires, an error will be reported.
        * `compression_type`:
          Set the compression type for the producer. By default, message
          payloads are not compressed. Supported compression types are
          `CompressionType.LZ4`, `CompressionType.ZLib`, `CompressionType.ZSTD` and `CompressionType.SNAPPY`.
          ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
          release in order to be able to receive messages compressed with ZSTD.
          SNAPPY is supported since Pulsar 2.4. Consumers will need to be at least at that
          release in order to be able to receive messages compressed with SNAPPY.
        * `max_pending_messages`:
          Set the max size of the queue holding the messages pending to receive
          an acknowledgment from the broker.
        * `max_pending_messages_across_partitions`:
          Set the max size of the queue holding the messages pending to receive
          an acknowledgment across partitions from the broker.
        * `block_if_queue_full`: Set whether `send_async` operations should
          block when the outgoing message queue is full.
        * `message_routing_mode`:
          Set the message routing mode for the partitioned producer. Default is
          `PartitionsRoutingMode.RoundRobinDistribution`,
          other option is `PartitionsRoutingMode.UseSinglePartition`
        * `lazy_start_partitioned_producers`:
          This config affects producers of partitioned topics only. It controls whether
          producers register and connect immediately to the owner broker of each partition
          or start lazily on demand. The internal producer of one partition is always
          started eagerly, chosen by the routing policy, but the internal producers of
          any additional partitions are started on demand, upon receiving their first
          message.
          Using this mode can reduce the strain on brokers for topics with large numbers of
          partitions and when the SinglePartition routing policy is used without keyed messages.
          Because producer connection can be on demand, this can produce extra send latency
          for the first messages of a given partition.
        * `properties`:
          Sets the properties for the producer. The properties associated with a producer
          can be used for identify a producer at broker side.
        * `batching_type`:
          Sets the batching type for the producer.
          There are two batching type: DefaultBatching and KeyBasedBatching.
            - Default batching
            incoming single messages:
            (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
            batched into single batch message:
            [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
            - KeyBasedBatching
            incoming single messages:
            (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
            batched into single batch message:
            [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
        * encryption_key:
           The key used for symmetric encryption, configured on the producer side
        * crypto_key_reader:
           Symmetric encryption class implementation, configuring public key encryption messages for the producer
           and private key decryption messages for the consumer
        """
        coro = self._producer(*args, **kwargs)
        return _ContextManager(coro)
