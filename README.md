# Feast Teradata Connector
[![test-offline-store](https://github.com/feast-dev/feast-custom-offline-store-demo/actions/workflows/test_custom_offline_store.yml/badge.svg?branch=main)](https://github.com/feast-dev/feast-custom-offline-store-demo/actions/workflows/test_custom_offline_store.yml)

## Offline Store

### Overview

This document demonstrates how developers can integrate `Teradata's offline store` for Feast.
Teradata's offline stores allow users to use any underlying data store as their offline feature store. Features can be retrieved from the offline store for model training, and can be materialized into the online feature store for use during model inference. 


### Why create an offline store?

Feast uses an offline store as the source of truth for features. These features can be retrieved from the offline store for model training. Typically, scalable data warehouses are used for this purpose.
 
Feast also materializes features from offline stores to an online store for low-latency lookup at model inference time. 

Feast comes with some offline stores built in, e.g, Parquet file, Redshift and Bigquery. However, users can develop their own offline stores by creating a class that implements the contract in the [OfflineStore class](https://github.com/feast-dev/feast/blob/5e61a6f17c3b52f20b449214a4bb56bafa5cfcbc/sdk/python/feast/infra/offline_stores/offline_store.py#L41).

### How to configure feature_store.yaml?

```yaml
project: <name of project>
registry: <registry>
provider: local
offline_store:
   type: feast_teradata.offline.teradata.TeradataOfflineStore
   host: <db host>
   database: <db name>
   user: <username>
   password: <password>
   log_mech: <connection mechanism>
```

### Example Feature Definitions

```python
import yaml
import pandas as pd
from datetime import timedelta
from feast import Entity, Field, FeatureView,FeatureService, PushSource,RequestSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int64, Float32
from feast_teradata.offline.teradata_source import TeradataSource

driver = Entity(name="driver", join_keys=["driver_id"])
project_name = yaml.safe_load(open("feature_store.yaml"))["project"]

driver_stats_source = TeradataSource(
    database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
    table=f"{project_name}_feast_driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(weeks=52 * 10),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    source=driver_stats_source,
    tags={"team": "driver_performance"},
)

```

Now to explain the different components:

* `TeradataSource`:  Data Source for features stored in Teradata (Enterprise or Lake) or accessible via a Foreign Table from Teradata (NOS, QueryGrid)
* `Entity`: A collection of semantically related features
* `Feature View`: A feature view is a group of feature data from a specific data source. Feature views allow you to consistently define features and their data sources, enabling the reuse of feature groups across a project

### Example OfflineStore usage

There are two different ways to test your offline store as explained below. 
But first there are a few mandatory steps to follow:

Importing the relevant entities and feature views
```python
from <path_to_repository.py> import <name_of_entity>, <name_of_feature_view>
```

Initialize the feature store:
```python
fs = FeatureStore("<path_to_feature_repository>/")
```

Applying the feature view and entity on the feature store
```python
fs.apply([<name_of_entity>, <name_of_feature_view>])
```


```python
        entity_sql = f"""
            SELECT
                "driver_id",
                "event_timestamp"
            FROM {fs.get_data_source(table_name).get_table_query_string()}
            WHERE "event_timestamp" BETWEEN '{start_date}' AND '{end_date}'
        """

    training_df = fs.get_historical_features(
        entity_df=entity_sql,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
    ).to_df()
```

## Online Store

### Overview

This repository demonstrates how developers can create their own custom online stores for Feast. 
Custom online stores allow users to use any underlying data store to store features for low-latency retrieval, typically needed during model inference.

### Why create an online store?

Feast materializes data to online stores for low-latency lookup at model inference time. Typically, key-value stores are used for the online stores, however relational databases can be used for this purpose as well.

Users can develop their own online stores by creating a class that implements the contract in the OnlineStore class.

### How to configure feature_store.yaml?

```yaml
project: <name of project>
registry: <registry>
provider: local
offline_store:
   type: feast_teradata.offline.teradata.TeradataOfflineStore
   host: <db host>
   database: <db name>
   user: <username>
   password: <password>
   log_mech: <connection mechanism>
```

### Example OnlineStore usage

There are a few mandatory steps to follow before we can test the online store:

Importing the relevant entities and feature views
```python
from <path_to_repository.py> import <name_of_entity>, <name_of_feature_view>
```

Initialize the feature store:
```python
fs = FeatureStore("<path_to_feature_repository>/")
```

Applying the feature view and entity on the feature store
```python
fs.apply([<name_of_entity>, <name_of_feature_view>])
```

```python
def fetch_online_features(store, source: str = ""):
    entity_rows = [
        # {join_key: entity_value}
        {
            "driver_id": 1001,
            "val_to_add": 1000,
            "val_to_add_2": 2000,
        },
        {
            "driver_id": 1002,
            "val_to_add": 1001,
            "val_to_add_2": 2002,
        },
    ]
    if source == "feature_service":
        features_to_fetch = store.get_feature_service("driver_activity_v1")
    elif source == "push":
        features_to_fetch = store.get_feature_service("driver_activity_v3")
    else:
        features_to_fetch = [
            "driver_hourly_stats:acc_rate",
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2",
        ]
    returned_features = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows,
    ).to_dict()
    for key, value in sorted(returned_features.items()):
        print(key, " : ", value)
```

The command below is used to incrementally materialize features in the online store. 
If there are no new features to be added, this command will essentially not be doing
anything. With feast `materialize_incremental`, the start time is either now — ttl 
(the ttl that we defined in our feature views) or the time of the most recent 
materialization. If you’ve materialized features at least once, then subsequent 
materializations will only fetch features that weren’t present in the store at 
the time of the previous materializations

```python
fs.materialize_incremental(end_date=datetime.now())
```

Next, while fetching the online features, we have two parameters `features` and
`entity_rows`. The `features` parameter is a list and can take any number of features
that are present in the `df_feature_view`. The example above shows all 4 features present
but these can be less than 4 as well. Secondly, the `entity_rows` parameter is also
a list and takes a dictionary of the form {feature_identifier_column: value_to_be_fetched}.
In our case, the column `driver_id` is used to uniquely identify the different rows
of the entity driver. We are currently fetching values of the features where driver_id
is equal to 5. We can also fetch multiple such rows using the format: 
`[{driver_id: val_1}, {driver_id: val_2}, .., {driver_id: val_n}]`
`[{driver_id: val_1}, {driver_id: val_2}, .., {driver_id: val_n}]`

## Release Notes

### 0.1

- Feature: Initial implementation of feast-teradata library
