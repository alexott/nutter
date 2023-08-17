"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

import os

__version__ = '0.1.35'

BUILD_NUMBER_ENV_VAR = 'NUTTER_BUILD_NUMBER'


def get_nutter_version():
    build_number = os.environ.get(BUILD_NUMBER_ENV_VAR)
    if build_number:
        return f'{__version__}.{build_number}'
    return __version__



def contains_test_prefix_or_surfix(name):
    if name is None:
        return False

    return name.lower().startswith('test_') or name.lower().endswith('_test')
