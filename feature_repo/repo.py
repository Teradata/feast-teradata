from datetime import timedelta

from feast import Entity, Field, FeatureView, ValueType
from feast.types import Float64


from feast_teradata.teradata import TeradataSource

flower_stats = TeradataSource(
    name="Iris_d_p",
    query="SELECT * FROM mw255040.iris_data",
    timestamp_field="event_timestamp"
)

flower = Entity(name="flower", join_keys=["flower_id"])

df_feature_view = FeatureView(
    name="df_feature_view",
    ttl=timedelta(days=3),
    entities=[flower],
    schema=[
        Field(name="sepal length (cm)", dtype=Float64),
        Field(name="sepal width (cm)", dtype=Float64),
        Field(name="petal length (cm)", dtype=Float64),
        Field(name="petal width (cm)", dtype=Float64),
        ],
    online=True,
    source=flower_stats
)
