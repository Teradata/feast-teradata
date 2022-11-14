from datetime import datetime
from typing import Sequence, List, Optional, Tuple, Dict, Callable, Any

import pytz
import itertools
from binascii import hexlify
from feast.usage import log_exceptions_and_usage
from feast import RepoConfig, FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from pydantic.typing import Literal
import pandas as pd
from feast.utils import to_naive_utc
from feast_teradata.teradata_utils import (
    get_conn,
    TeradataConfig
)
from teradataml import (
    copy_to_sql,
    VARBYTE,
    VARCHAR,
    TIMESTAMP,
    DataFrame
)

types_dict = {
    "entity_feature_key": VARBYTE(512),
    "entity_key": VARBYTE(512),
    "feature_name": VARCHAR(512),
    "value": VARBYTE(1024),
    "event_ts": TIMESTAMP,
    "created_ts": TIMESTAMP
}


class TeradataOnlineStoreConfig(TeradataConfig):
    type: Literal[
        "feast_teradata.online.teradata.TeradataOnlineStore"
    ] = "feast_teradata.online.teradata.TeradataOnlineStore"


class TeradataOnlineStore(OnlineStore):

    @log_exceptions_and_usage(online_store="teradata")
    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        assert isinstance(config.online_store, TeradataOnlineStoreConfig)

        dfs = [None] * len(data)
        for i, (entity_key, values, timestamp, created_ts) in enumerate(data):
            df = pd.DataFrame(
                columns=[
                    "entity_feature_key",
                    "entity_key",
                    "feature_name",
                    "value",
                    "event_ts",
                    "created_ts",
                ],
                index=range(0, len(values)),
            )

            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            for j, (feature_name, val) in enumerate(values.items()):
                df.loc[j, "entity_feature_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                ) + bytes(feature_name, encoding="utf-8")
                df.loc[j, "entity_key"] = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                df.loc[j, "feature_name"] = feature_name
                df.loc[j, "value"] = val.SerializeToString()
                df.loc[j, "event_ts"] = timestamp
                df.loc[j, "created_ts"] = created_ts

            dfs[i] = df

        if dfs:
            agg_df = pd.concat(dfs)

            # This combines both the data upload plus the overwrite in the same transaction
            with get_conn(config.online_store).connect() as conn:
                copy_to_sql(df=agg_df,
                            table_name=f"{config.project}_{table.name}_t",
                            if_exists="replace",
                            types=types_dict,
                            primary_index="entity_feature_key")

                query = f"""
                        MERGE INTO {config.project}_{table.name} tar
                        USING {config.project}_{table.name}_t src
                           ON tar.entity_feature_key=src.entity_feature_key AND tar.entity_key = src.entity_key AND tar.feature_name = src.feature_name 
                        WHEN MATCHED THEN
                           UPDATE SET "value" = src."value", event_ts = src.event_ts, created_ts = src.created_ts
                        WHEN NOT MATCHED THEN
                               INSERT (entity_feature_key, entity_key, feature_name, "value", event_ts, created_ts) 
                                   VALUES (src.entity_feature_key, src.entity_key, src.feature_name, src."value", src.event_ts, src.created_ts)
                        """
                conn.execute(query)

            if progress:
                progress(len(data))

        return None

    @log_exceptions_and_usage(online_store="teradata")
    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: List[str],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, TeradataOnlineStoreConfig)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        entity_fetch_str = ",".join(
            [
                (
                        "TO_BYTES("
                        + hexlify(
                            serialize_entity_key(
                                combo[0],
                                entity_key_serialization_version=config.entity_key_serialization_version,
                            )
                            + bytes(combo[1], encoding="utf-8")
                        ).__str__()[1:]
                        + ",'base16')"
                )
                for combo in itertools.product(entity_keys, requested_features)
            ]
        )

        with get_conn(config.online_store).connect() as conn:
            query = f"""
                    SELECT
                        "entity_key", "feature_name", "value", "event_ts"
                    FROM
                        "{config.project}_{table.name}"
                    WHERE
                        "entity_feature_key" IN ({entity_fetch_str})
                """
            df = DataFrame.from_query(query).to_pandas()

        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            res = {}
            res_ts = None
            for index, row in df[df["entity_key"] == entity_key_bin].iterrows():
                val = ValueProto()
                val.ParseFromString(row["value"])
                res[row["feature_name"]] = val
                res_ts = row["event_ts"].to_pydatetime()

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    @log_exceptions_and_usage(online_store="snowflake")
    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool,
    ):
        assert isinstance(config.online_store, TeradataOnlineStoreConfig)

        with get_conn(config.online_store).connect() as conn:
            for table in tables_to_keep:
                query = f"""
                        CREATE TABLE {config.project}_{table.name} (
                            "entity_feature_key" VARBYTE(512),
                            "entity_key" VARBYTE(512),
                            "feature_name" VARCHAR(512),
                            "value" VARBYTE(1024),
                            "event_ts" TIMESTAMP,
                            "created_ts" TIMESTAMP
                        )
                    """
                conn.execute(query)

            for table in tables_to_delete:
                query = f"""DROP TABLE {config.project}_{table.name}"""
                conn.execute(query)

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity],
    ):
        assert isinstance(config.online_store, TeradataOnlineStoreConfig)

        with get_conn(config.online_store).connect() as conn:
            for table in tables:
                query = f"""DROP TABLE {config.project}_{table.name}"""
                conn.execute(query)


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"
