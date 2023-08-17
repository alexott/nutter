"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

import uuid
import datetime
from .apiclientresults import ExecuteNotebookResult, WorkspacePath
import logging
from .utils import get_nutter_version

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, Task

MIN_TIMEOUT = 10


def databricks_client():
    return DatabricksAPIClient()


class DatabricksAPIClient(object):
    """
    """

    def __init__(self):
        self.min_timeout = MIN_TIMEOUT
        self.dbclient = WorkspaceClient(product="nutter", product_version=get_nutter_version())

    def list_notebooks(self, path):
        workspace_objects = self.list_objects(path)
        notebooks = workspace_objects.notebooks
        return notebooks

    def list_objects(self, path):
        objects = self.dbclient.workspace.list(path)
        logging.debug(f'Creating WorkspacePath for path {path}')
        logging.debug(f'List response: \n\t{objects}')

        workspace_path_obj = WorkspacePath.from_api_response(objects)
        logging.debug('WorkspacePath created')

        return workspace_path_obj

    def execute_notebook(self, notebook_path, cluster_id, timeout=120,
                         notebook_params=None):
        if not notebook_path:
            raise ValueError("empty path")
        if not cluster_id:
            raise ValueError("empty cluster id")
        if timeout < self.min_timeout:
            raise ValueError(
                f"Timeout must be greater than {self.min_timeout}")
        if notebook_params is not None:
            if not isinstance(notebook_params, dict):
                raise ValueError("Parameters must be in the form of a dictionary (See "
                                 "#run-single-test-notebook section in README)")

        name = str(uuid.uuid1())
        ntask = Task(notebook_task=NotebookTask(notebook_path, base_parameters=notebook_params),
                     existing_cluster_id=cluster_id, task_key="a")

        run = self.dbclient.jobs.submit_and_wait(tasks=[ntask], run_name=name,
                                                 timeout=datetime.timedelta(seconds=timeout))

        return ExecuteNotebookResult.from_job_output(run, self.dbclient)


class InvalidConfigurationException(Exception):
    pass


class TimeOutException(Exception):
    pass
