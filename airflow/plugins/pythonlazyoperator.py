"""An airflow plugin that extends PythonOperator

This module implements an airflow operator that extends the PythonOperator,
allowing the definition of python callback functions by using their path
instead of requiring them to be previously imported.

"""

from importlib import import_module
import os
import sys

from airflow.operators import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


def lazy_import(path):
    """Import a python named object dynamically.

    Parameters
    ----------
    path: str
        Path to import. It can be one of:
        * A python path, like ``mypackage.my_callback_function``
        * A filesystem path, with the indication of the python name to import
          being given using a colon, like ``~/tasks.py:do_something``

    """

    try:
        python_name = _lazy_import_filesystem_path(path)
    except (RuntimeError, KeyError):
        python_name = _lazy_import_python_path(path)
    return python_name


class PythonLazyOperator(PythonOperator):

    @apply_defaults
    def __init__(self, python_callable, *args, **kwargs):
        super(PythonLazyOperator, self).__init__(
            python_callable, *args, **kwargs)
        self.python_callable = lazy_import(python_callable)


class PythonLazyOperatorPlugin(AirflowPlugin):
    name = "python_lazy_operator"
    operators = [
        PythonLazyOperator,
    ]


def _lazy_import_filesystem_path(path):
    """Lazily import a python name from a filesystem path

    Parameters
    ----------
    path: str
        A colon separated string with the path to the module to load and the
        name of the object to import

    Returns
    -------
    The imported object

    Raises
    ------
    KeyError
        If the name is not found on the loaded python module
    RuntimeError
        If the path is not valid

    """

    filesystem_path, python_name = path.rpartition(":")[::2]
    full_path = os.path.abspath(os.path.expanduser(filesystem_path))
    if os.path.isfile(full_path):
        dirname, filename = os.path.split(full_path)
        module_name = os.path.splitext(filename)[0]
        sys.path.append(dirname)
        loaded_module = import_module(module_name)
        return loaded_module.__dict__[python_name]
    else:
        raise RuntimeError("Invalid path {!r}".format(full_path))


def _lazy_import_python_path(path):
    module_path, python_name = path.rpartition(".")[::2]
    loaded_module = import_module(module_path)
    return loaded_module.__dict__.get(python_name)

