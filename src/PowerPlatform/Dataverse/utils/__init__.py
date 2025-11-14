# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Utilities and adapters for the Dataverse SDK.

This module contains adapters (like Pandas integration).
"""

from .pandas_adapter import PandasODataClient

__all__ = ["PandasODataClient"]