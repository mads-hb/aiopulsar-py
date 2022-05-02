# aiopulsar-py
**aiopulsar-py** is a Python 3.5+ module that makes it possible to interact with pulsar servers with asyncio. 
**aiopulsar-py** serves as an asynchronous wrapper for the 
[official python pulsar-client](https://pulsar.apache.org/docs/en/client-libraries-python/) and preserves the look and 
feel of the original pulsar-client. It is written using the async/await syntax and hence not compatible with Python
versions older than 3.5. Internally, aiopulsar-py employs threads to avoid blocking the event loop.

**aiopulsar-py** takes inspiration from other asyncio wrappers released in the 
[aio-libs project](https://github.com/aio-libs).
## Basic example
**aiopulsar-py** is built around the [python pulsar-client](https://pulsar.apache.org/docs/en/client-libraries-python/)
and provides the same api. You just need to use asynchronous context managers and await for every method. Setting up a
pulsar client that can be used to create readers, producers and consumers requires a call to the ``aiopulsar.connect`` 
method.
````python
import asyncio
import aiopulsar
import pulsar


async def test_example():
    topic = "persistent://some-test-topic/"

    async with aiopulsar.connect("localhost") as client:
        async with client.subscribe(
            topic=topic,
            subscription_name='my-subscription',
            consumer_name="my-consumer",
            initial_position=pulsar.InitialPosition.Earliest,
        ) as consumer:
            while True:
                msg = await consumer.receive()
                print(msg)

loop = asyncio.get_event_loop()
loop.run_until_complete(test_example())
````
## Install
**aiopulsar-py** cannot be installed on windows systems since the wrapped 
[pulsar-client](https://pulsar.apache.org/docs/en/client-libraries-python/) library only functions on Linux and MacOS.
The package is available on PyPi and can be installed with:
````shell
pip install aiopulsar-py
````
## Contributing
You can contribute to the project by reporting an issue. This is done via GitHub. Please make sure to include 
information on your environment and please make sure that you can express the issue with a reproducible test case. 

You can also contribute by making pull requests. To install the project please use poetry:
````shell
poetry install
````
The project relies on ``mypy``, `black` and `flake8` and these are configured as git hooks. 
To configure the git hooks run:
````shell
poetry run githooks setup
````
Every time the git hooks are changed in the ``[tool.githooks]`` section of `pyproject.toml` you will need to set up
the git hooks again with the command above.