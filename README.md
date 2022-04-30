# AioPulsar
AioPulsar is an asynchronous wrapper for the official python pulsar-client.

## Contributing
To contribute to the project, you need to have poetry installed. Install the dependencies with:
````shell
poetry install
````
The project relies on ``mypy``, `black` and `flake8` and these are configured as git hooks. To configure the git hooks run:
````shell
poetry run githooks setup
````
Every time the git hooks are changed in the ``[tool.githooks]`` section of `pyproject.toml` you will need to run the command above again.