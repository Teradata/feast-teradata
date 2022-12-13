import contextlib
from dataclasses import asdict
from datetime import datetime
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    KeysView,
    List,
    Optional,
    Tuple,
    Union,
)
import pyarrow
import numpy as np
import pandas as pd
import pyarrow as pa
from jinja2 import BaseLoader, Environment
from pydantic.typing import Literal
from pytz import utc

from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils

from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.registry import Registry
from feast_teradata.teradata_utils import (
    get_conn,
    TeradataConfig,
    teradata_type_to_feast_value_type,
)
from feast_teradata.offline.teradata_source import (
    TeradataSource,
    df_to_teradata_table
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage


class TeradataOfflineStoreConfig(TeradataConfig):
    type: Literal[
        "feast_teradata.offline.teradata.TeradataOfflineStore"
    ] = "feast_teradata.offline.teradata.TeradataOfflineStore"


class TeradataOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="teradata")
    def pull_latest_from_table_or_query(
            config: RepoConfig,
            data_source: DataSource,
            join_key_columns: List[str],
            feature_name_columns: List[str],
            timestamp_field: str,
            created_timestamp_column: Optional[str],
            start_date: datetime,
            end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, TeradataOfflineStoreConfig)
        assert isinstance(data_source, TeradataSource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(_append_alias(join_key_columns, "a"))
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                    "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(_append_alias(timestamps, "a")) + " DESC"
        a_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "a")
        )
        b_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "b")
        )

        query = f"""
            SELECT
                {b_field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE a."{timestamp_field}" BETWEEN '{start_date}' AND '{end_date}'
            ) b
            WHERE _feast_row = 1
            """


        return TeradataRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="teradata")
    def get_historical_features(
            config: RepoConfig,
            feature_views: List[FeatureView],
            feature_refs: List[str],
            entity_df: Union[pd.DataFrame, str],
            registry: Registry,
            project: str,
            full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, TeradataOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, TeradataSource)

        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(config, entity_df, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            query_context_dict = [asdict(context) for context in query_context]
            # Hack for query_context.entity_selections to support uppercase in columns
            for context in query_context_dict:
                context["entity_selections"] = [
                    f'''"{entity_selection.replace(' AS ', '" AS "')}\"'''
                    for entity_selection in context["entity_selections"]
                ]

            try:
                yield build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
            finally:
                if table_name:
                    with get_conn(config.offline_store).connect() as conn:
                        conn.execute(f"DROP TABLE {table_name}")

        return TeradataRetrievalJob(
            query=query_generator,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="teradata")
    def pull_all_from_table_or_query(
            config: RepoConfig,
            data_source: DataSource,
            join_key_columns: List[str],
            feature_name_columns: List[str],
            timestamp_field: str,
            start_date: datetime,
            end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, TeradataOfflineStoreConfig)
        assert isinstance(data_source, TeradataSource)
        from_expression = data_source.get_table_query_string()

        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT {field_string}
            FROM {from_expression} AS paftoq_alias
            WHERE "{timestamp_field}" BETWEEN '{start_date}'::timestamptz AND '{end_date}'::timestamptz
        """

        return TeradataRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )
    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        assert isinstance(config.offline_store, TeradataOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, TeradataSource)

        pa_schema, column_names = offline_utils.get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        if table.schema != pa_schema:
            table = table.cast(pa_schema)

        table_df = table.to_pandas()
        with get_conn(config.offline_store).connect() as conn:
            table_df.to_sql(name=feature_view.batch_source.name,
                            con=conn,
                            if_exists="append",
                            index=False)

class TeradataRetrievalJob(RetrievalJob):
    def __init__(
            self,
            query: Union[str, Callable[[], ContextManager[str]]],
            config: RepoConfig,
            full_feature_names: bool,
            on_demand_feature_views: Optional[List[OnDemandFeatureView]],
            metadata: Optional[RetrievalMetadata] = None,
    ):
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    def to_sql(self) -> str:
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self) -> pa.Table:
        with self._query_generator() as query:
            with get_conn(self.config.offline_store).raw_connection().cursor() as cur:
                cur.execute(query)
                fields = [
                    (c[0], teradata_type_to_feast_value_type(c[1]))
                    for c in cur.description
                ]
                data = cur.fetchall()
                schema = pa.schema(fields)

                data_transposed: List[List[Any]] = []
                for col in range(len(fields)):
                    data_transposed.append([])
                    for row in range(len(data)):
                        data_transposed[col].append(data[row][col])
                table = pa.Table.from_arrays(
                    [pa.array(row) for row in data_transposed], schema=schema
                )

            return table

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        raise NotImplementedError("Not yet implemented")


def _get_entity_df_event_timestamp_range(
        entity_df: Union[pd.DataFrame, str],
        entity_df_event_timestamp_col: str,
        config: RepoConfig,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
                                    :, entity_df_event_timestamp_col
                                    ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        with get_conn(config.offline_store).raw_connection().cursor() as cur:
            cur.execute(
                f"SELECT MIN({entity_df_event_timestamp_col}) AS min_ts, MAX({entity_df_event_timestamp_col}) AS max_ts FROM ({entity_df}) as tmp_alias"
            ),
            res = cur.fetchone()
        entity_df_event_timestamp_range = (res[0], res[1])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    return [f'{alias}."{field_name}"' for field_name in field_names]


def build_point_in_time_query(
        feature_view_query_contexts: List[dict],
        left_table_query_string: str,
        entity_df_event_timestamp_col: str,
        entity_df_columns: KeysView[str],
        query_template: str,
        full_feature_names: bool = False,
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for teradata"""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)

    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f'{fv["name"]}__{fv["field_mapping"].get(feature, feature)}'
                if full_feature_names
                else fv["field_mapping"].get(feature, feature)
            )
            for fv in feature_view_query_contexts
            for feature in fv["features"]
        ]
    )

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv["entities"]]
        ),
        "featureviews": feature_view_query_contexts,
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
    }

    query = template.render(template_context)
    return query


def _upload_entity_df(
        config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Postgres
        df_to_teradata_table(config.offline_store, entity_df, table_name)
    elif isinstance(entity_df, str):
        with get_conn(config.offline_store).raw_connection().cursor() as cur:
            cur.execute(f"CREATE TABLE {table_name} AS ({entity_df}) with data")

    #     # If the entity_df is a string (SQL query), create a Postgres table out of it
    #         cur.execute(f"CREATE TABLE {table_name} AS ({entity_df})")
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_schema(
        entity_df: Union[pd.DataFrame, str],
        config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        df_query = f"({entity_df}) AS sub"
        return get_query_schema(config.offline_store, df_query)
    else:
        raise InvalidEntityType(type(entity_df))


def get_query_schema(config: TeradataConfig, sql_query: str) -> Dict[str, np.dtype]:
    """
    We'll use the statement when we perform the query rather than copying data to a
    new table
    """
    with get_conn(config).connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {sql_query}",
            conn,
        )
    return dict(zip(df.columns, df.dtypes))


# Copied from the Feast Redshift offline store implementation
# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:
# MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/redshift.py

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
Compute a deterministic hash for the `left_table_query_string` that will be used throughout
all the logic as the field to GROUP BY the data
*/
WITH "entity_dataframe" AS (
    SELECT a.*,
        "{{entity_df_event_timestamp_col}}" AS "entity_timestamp"
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" AS VARCHAR(256)) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR(256))
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR(1000)) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM "{{ left_table_query_string }}" a
),
{% for featureview in featureviews %}
"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
       {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
    FROM "entity_dataframe"
    GROUP BY
        {{ featureview.entities | map('tojson') | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        "entity_timestamp",
        "{{featureview.name}}__entity_row_unique_id"
),
/*
This query template performs the point-in-time correctness join for a single feature set table
to the provided entity table.
1. We first join the current feature_view to the entity dataframe that has been passed.
This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously
The output of this CTE will contain all the necessary information and already filtered out most
of the data that is not relevant.
*/
"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.timestamp_field }}" as "event_timestamp",
        {{'"' ~ featureview.created_timestamp_column ~ '" as "created_timestamp",' if featureview.created_timestamp_column else '' }}
        {{featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} as base
    WHERE "{{ featureview.timestamp_field }}" <= '{{ featureview.max_event_timestamp }}'
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.timestamp_field }}" >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),
"{{ featureview.name }}__base" AS (
    SELECT
        "subquery".*,
        "entity_dataframe"."entity_timestamp",
        "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS "subquery"
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS "entity_dataframe"
    ON 1=1
        AND "subquery"."event_timestamp" <= "entity_dataframe"."entity_timestamp"
        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '0 00:00:01' day to second /* 
Uses TD specific function day to second to make sure any value for TTL works (up till 27 years converted to seconds). Takes an additional day parameter to convert the value to seconds
 */
        {% endif %}
        {% for entity in featureview.entities %}
        AND "subquery"."{{ entity }}" = "entity_dataframe"."{{ entity }}"
        {% endfor %}
),
/*
2. If the `created_timestamp_column` has been set, we need to
deduplicate the data first. This is done by calculating the
`MAX(created_at_timestamp)` for each event_timestamp.
We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        "event_timestamp",
        MAX("created_timestamp") AS "created_timestamp"
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", "event_timestamp"
),
{% endif %}
/*
3. The data has been filtered during the first CTE "*__base"
Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        "event_timestamp",
        {% if featureview.created_timestamp_column %}"created_timestamp",{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        SELECT b.*,
            ROW_NUMBER() OVER(
                PARTITION BY b."{{featureview.name}}__entity_row_unique_id"
                ORDER BY b."event_timestamp" DESC{% if featureview.created_timestamp_column %},b."created_timestamp" DESC{% endif %}
            ) AS "row_number"
        FROM "{{ featureview.name }}__base" b
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup" bb
            ON b."{{featureview.name}}__entity_row_unique_id" = bb."{{featureview.name}}__entity_row_unique_id"
            AND b."event_timestamp" = bb."event_timestamp"
            AND b."created_timestamp" = bb."created_timestamp"
        {% endif %}
    ) as c
    WHERE "row_number" = 1
),
/*
4. Once we know the latest value of each feature for a given timestamp,
we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT "base".*
    FROM "{{ featureview.name }}__base" AS "base"
    INNER JOIN "{{ featureview.name }}__latest"
    ON "base"."{{featureview.name}}__entity_row_unique_id" = "{{ featureview.name }}__latest"."{{featureview.name}}__entity_row_unique_id"
    AND "base"."event_timestamp" = "{{ featureview.name }}__latest"."event_timestamp"

    {% if featureview.created_timestamp_column %}
        AND
        "base"."created_timestamp" = "{{ featureview.name }}__latest"."created_timestamp"
    {% endif %}
){% if loop.last %}{% else %}, {% endif %}
{% endfor %}
/*
Joins the outputs of multiple time travel joins to a single table.
The entity_dataframe dataset being our source of truth here.
*/
SELECT "{{ final_output_feature_names | join('", "')}}"
FROM "entity_dataframe"
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__cleaned"
) "{{ featureview.name }}__cleaned" ON "entity_dataframe"."{{featureview.name}}__entity_row_unique_id"="{{ featureview.name }}__cleaned"."{{featureview.name}}__entity_row_unique_id"
{% endfor %}
"""
