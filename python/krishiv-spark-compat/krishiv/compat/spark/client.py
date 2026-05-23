"""Spark Connect gRPC client using generated Spark 3.5 protos."""

from __future__ import annotations

import uuid
from typing import List

import grpc

from krishiv.compat.spark._proto.spark.connect import base_pb2, base_pb2_grpc, relations_pb2


class SparkConnectClient:
    def __init__(self, host: str, port: int, timeout_sec: float = 30.0) -> None:
        self._timeout = timeout_sec
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = base_pb2_grpc.SparkConnectServiceStub(self._channel)

    def execute_sql(self, query: str, session_id: str | None = None) -> List[bytes]:
        session_id = session_id or str(uuid.uuid4())
        user = base_pb2.UserContext(user_id="krishiv", user_name="krishiv")
        sql_rel = relations_pb2.Relation(
            sql=relations_pb2.SQL(query=query)
        )
        plan = base_pb2.Plan(root=sql_rel)
        req = base_pb2.ExecutePlanRequest(
            session_id=session_id,
            user_context=user,
            plan=plan,
        )
        batches: List[bytes] = []
        for resp in self._stub.ExecutePlan(req, timeout=self._timeout):
            if resp.HasField("arrow_batch"):
                batches.append(resp.arrow_batch.data)
        return batches

    def spark_version(self, session_id: str | None = None) -> str:
        session_id = session_id or str(uuid.uuid4())
        user = base_pb2.UserContext(user_id="krishiv", user_name="krishiv")
        req = base_pb2.AnalyzePlanRequest(
            session_id=session_id,
            user_context=user,
            spark_version=base_pb2.AnalyzePlanRequest.SparkVersion(),
        )
        resp = self._stub.AnalyzePlan(req, timeout=self._timeout)
        return resp.spark_version.version

    def close(self) -> None:
        self._channel.close()
