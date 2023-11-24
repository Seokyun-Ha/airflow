# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.providers.databricks.hooks.databricks import DatabricksHook, ClusterState
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabrickClusterSensor(BaseSensorOperator):
    """
    Waits for a Redshift cluster to reach a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:DatabrickClusterSensor`

    :param cluster_id: The identifier for the cluster being pinged.
    :param target_state: The cluster status desired.
    """

    template_fields: Sequence[str] = ("cluster_id", "target_state")

    def __init__(
        self,
        *,
        cluster_id: str,
        target_state: ClusterState = "RUNNING",
        databricks_conn_id: str = DatabricksHook.default_conn_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_id = cluster_id
        self.target_state = target_state
        self.databricks_conn_id = databricks_conn_id

    def poke(self, context: Context):
        current_state = self.hook.get_cluster_state(self.cluster_id)
        self.log.info(
            "Poked Databricks cluster %s for status '%s', found status '%s'",
            self.cluster_id,
            self.target_state,
            current_state,
        )
        return current_state.state == self.target_state

    @cached_property
    def hook(self) -> DatabricksHook:
        return DatabricksHook(databricks_conn_id=self.databricks_conn_id)
