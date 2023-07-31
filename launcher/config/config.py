import os

from dotenv import dotenv_values


class Config:
    """Load the configuration from a file and present config and env in unified way."""

    def __init__(self, filename=None):
        """Init."""
        # assume local env but it can be overwritten by the config file or envirnoment variable
        self.config = {"env": "local"}

        if filename:
            # add the values from the file to the config
            self.update(dotenv_values(filename))

    def update(self, values: dict):
        """Update the configuration values."""
        self.config.update(values)

    def get(self, key: str):
        """Try to get environment variable or config as alternative."""
        # check if present as env variable
        if key in os.environ:
            return os.environ[key]
        else:
            return self.config.get(key)
