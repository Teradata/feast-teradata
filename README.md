# Feast Teradata Connector
[![feast-teradata tests](https://github.com/Teradata/feast-teradata/actions/workflows/ci-integeration-tests.yml/badge.svg?branch=master)](https://github.com/Teradata/feast-teradata/actions/workflows/ci-integeration-tests.yml)

## Overview

We recommend you familiarize yourself with the terminology and concepts of feast by reading the official [feast documentation](https://docs.feast.dev/). 

The `feast-teradata` library adds support for Teradata as 
- OfflineStore 
- OnlineStore

Additional, the using Teradata as the registry is already supported via the `registry_type: sql` and included in our examples. This means that everything is located in Teradata. However, depending on the requirements, installation, etc, this can be mixed and matched with other systems as appropriate.  

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

```bash
python demo/test_workflow.py
```


## Release Notes

### 1.0.0

- Feature: Initial implementation of feast-teradata library
