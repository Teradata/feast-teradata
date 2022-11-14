from feast import FeatureStore
from feature_repo.repo import flower, df_feature_view
from datetime import datetime
from teradataml import create_context, get_context

import getpass
import pandas as pd


def test_offline_sql_as_entity_df():
    rs = fs.get_historical_features(
        entity_df="SELECT event_timestamp, flower_id FROM iris_data",
        features=[
            "df_feature_view:sepal length (cm)",
            "df_feature_view:sepal width (cm)",
            "df_feature_view:petal length (cm)",
            "df_feature_view:petal width (cm)"
        ]
        )
    rs = rs.to_df()
    print(rs)


def test_offline_pandas_df_as_entity_df():
    username = input("Username: ")
    password = getpass.getpass("Password: ")
    create_context(host="tdprd.td.teradata.com", username=username, password=password, logmech="LDAP")

    target_df = pd.read_sql("SELECT event_timestamp, flower_id FROM iris_data", get_context())

    rs = fs.get_historical_features(
        entity_df=target_df,
        features=[
            "df_feature_view:sepal length (cm)",
            "df_feature_view:sepal width (cm)",
            "df_feature_view:petal length (cm)",
            "df_feature_view:petal width (cm)"
        ]
        )
    rs = rs.to_df()
    print(rs)


def test_online():

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


fs = FeatureStore("feature_repo/")
try:
    fs.apply([flower, df_feature_view])

    # test_offline_sql_as_entity_df()

    test_online()

finally:
    fs.teardown()
