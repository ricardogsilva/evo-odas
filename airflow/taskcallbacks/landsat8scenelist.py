"""Callback functions for airflow tasks

This module holds functions used by the tasks of the airflow DAG that updates
the landsat scene list database.
"""

import csv
import zlib

import requests


def update_database(conf, *args, **kwargs):
    # download the scene list csv file
    # un-gz it
    # update the postgres database with new scene list records
    scene_list_url = conf.get("landsat8", "scene_list_url")
    raise NotImplementedError
