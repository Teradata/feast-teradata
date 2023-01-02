# Feast Teradata Connector
[![feast-teradata tests](https://github.com/Teradata/feast-teradata/actions/workflows/ci-integeration-tests.yml/badge.svg)](https://github.com/Teradata/feast-teradata/actions/workflows/ci-integeration-tests.yml)

## Overview

We recommend you familiarize yourself with the terminology and concepts of feast by reading the official [feast documentation](https://docs.feast.dev/). 

The `feast-teradata` library adds support for Teradata as 
- OfflineStore 
- OnlineStore

Additional, using Teradata as the registry (catalog) is already supported via the `registry_type: sql` and included in our examples. This means that everything is located in Teradata. However, depending on the requirements, installation, etc, this can be mixed and matched with other systems as appropriate.  

## Getting Started

To get started, install the `feast-teradata` library

```bash
pip install feast-teradata
```

Let's create a simple feast setup with Teradata using the standard drivers dataset. Note that you cannot use `feast init` as this command only works for templates which are part of the core feast library. We intend on getting this library merged into feast core eventually but for now, you will need to use the following cli command for this specific task. All other `feast` cli commands work as expected. 

```bash
feast-td init-repo
```

This will then prompt you for the required information for the Teradata system and upload the example dataset. Let's assume you used the repo name `demo` when running the above command. You can find the repository files along with a file called `test_workflow.py`. Running this `test_workflow.py` will execute a complete workflow for feast with Teradata as the Registry, OfflineStore and OnlineStore. 

```
demo/
    feature_repo/
        driver_repo.py
        feature_store.yml
    test_workflow.py
```

From within the `demo/feature_repo` directory, execute the following feast command to apply (import/update) the repo definition into the registry. You will be able to see the registry metadata tables in the teradata database after running this command.

```bash
feast apply
```

To see the registry information in the feast ui, run the following command. Note the --registry_ttl_sec is important as by default it polls every 5 seconds. 

```bash
feast ui --registry_ttl_sec=120
```


## Example Usage

Now, lets batch read some features for training taking only entities for which we have seen an event for in the last `60` days. This filter could be on anything that is relevant for the entity (population) selection for the given training dataset.

```python
from feast import FeatureStore


store = FeatureStore(repo_path="feature_repo")

training_df = store.get_historical_features(
    entity_df=f"""
            SELECT
                driver_id,
                event_timestamp
            FROM demo_feast_driver_hourly_stats
            WHERE event_timestamp BETWEEN (CURRENT_TIMESTAMP - INTERVAL '60' DAY) AND CURRENT_TIMESTAMP
        """,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips"
    ],
).to_df()
print(training_df.head())
```

To get the features for batch scoring, we would only change the `entity_df` sql query to select the list of `driver_id` to score on. Additionally, the `event_timestamp` can be the current timestamp for example in this scenario. 

```sql
SELECT
    "driver_id",
    CURRENT_TIMESTAMP AS "event_timestamp"
FROM <relevant-entity-table>
WHERE <relevant predicates>
```

To see a complete (but not real-world), end-to-end example workflow example, see the `demo/test_workflow.py` script. This script is used for testing the complete feast functionality.


## Release Notes

### 1.0.1

- Doc: Improve README with better getting started information. 
- Fix: Remove pytest from requirements.txt


### 1.0.0

- Feature: Initial implementation of feast-teradata library
