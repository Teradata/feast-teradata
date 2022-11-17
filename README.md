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
from datetime import timedelta
from feast import Entity, Field, FeatureView
from feast.types import Float64, Int64
from feast_teradata.offline.teradata_source import TeradataSource

flower_stats = TeradataSource(
    name="iris_ds",
    query="SELECT * FROM iris_data",
    timestamp_field="event_timestamp"
)

flower = Entity(name="flower", join_keys=["flower_id"])

df_feature_view = FeatureView(
    name="df_feature_view",
    ttl=timedelta(days=3),
    entities=[flower],
    schema=[
        Field(name="flower_id", dtype=Int64),
        Field(name="sepal length (cm)", dtype=Float64),
        Field(name="sepal width (cm)", dtype=Float64),
        Field(name="petal length (cm)", dtype=Float64),
        Field(name="petal width (cm)", dtype=Float64),
        ],
    online=True,
    source=flower_stats
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
rs = fs.get_historical_features(
    entity_df="SELECT event_timestamp, flower_id FROM iris_data",
    features=[
        "df_feature_view:sepal length (cm)",
        "df_feature_view:sepal width (cm)",
        "df_feature_view:petal length (cm)",
        "df_feature_view:petal width (cm)"
    ]
    ).to_df()
```

Another pathway could be the following:

```python
from teradataml import create_context, get_context

username = <username>
password = <password>
hostname = <hostname>
logmech = <connection mechanism>
create_context(host=hostname, username=username, password=password, logmech=logmech)

target_df = pd.read_sql("SELECT event_timestamp, flower_id FROM iris_data", get_context())

rs = fs.get_historical_features(
    entity_df=target_df,
    features=[
        "df_feature_view:sepal length (cm)",
        "df_feature_view:sepal width (cm)",
        "df_feature_view:petal length (cm)",
        "df_feature_view:petal width (cm)"
    ]
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
fs.materialize_incremental(end_date=datetime.now())

feature_vector = fs.get_online_features(
    features=[
        "df_feature_view:sepal length (cm)",
        "df_feature_view:sepal width (cm)",
        "df_feature_view:petal length (cm)",
        "df_feature_view:petal width (cm)"
    ], entity_rows=[{"flower_id": 5}]
    ).to_dict()

print(feature_vector)
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
In our case, the column `flower_id` is used to uniquely identify the different rows
of the entity flower. We are currently fetching values of the features where flower_id
is equal to 5. We can also fetch multiple such rows using the format: 
`[{flower_id: val_1}, {flower_id: val_2}, .., {flower_id: val_n}]`