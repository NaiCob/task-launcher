import importlib
import logging

from launcher.config.config import Config
from launcher.logging.config import init_logging
from launcher.data_provider.local_data_provider import LocalDataProvider

init_logging()

logger = logging.getLogger("bootstrap")


def _data_providers(conf):
    return {
        "data_provider": LocalDataProvider(conf),
    }

environments = {
    "local": _data_providers
}


def _update_conf(conf: dict, env: str, module: str) -> None:
    """
    Update configuration.

    Parameters
    ----------
    conf: dict
        Initial configuration
    env: str
        Target environment
    module: str
        Target config module name to be used for update

    """
    try:
        extra_config_module = importlib.import_module(module)
        conf.update(extra_config_module.config)
        env_config = f"config_{env}"
        if env_config in extra_config_module.__dict__:
            conf.update(extra_config_module.__dict__[env_config])
    except ModuleNotFoundError as err:
        logger.info(err)


def run(task: str, template: str, conf: Config) -> None:
    """
    Run the task and set up providers.

    Parameters
    ----------
    task: str
        Task name
    country: str
        Target country
    division: str
        Target division
    conf: Config
        Config used for running the task

    """
    env = conf.get("env")
    _update_conf(conf, env, "launcher.templates.config")
    _update_conf(conf, env, f"launcher.templates.{template}.config")

    task_module = importlib.import_module(f"launcher.templates.{template}.{task}")
    providers = environments[env](conf)

    # run the run function from the module
    task_module.run(providers["data_provider"], conf)
