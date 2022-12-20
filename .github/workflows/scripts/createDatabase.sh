#!/usr/bin/env bash

set -e

# add bteq to path
export PATH=$PATH:"/Users/runner/Library/Application Support/teradata/client/17.20/bin/"

cat << EOF > /tmp/pytestdatabase.bteq
.SET EXITONDELAY ON MAXREQTIME 20
.logon 127.0.0.1/dbc,dbc
CREATE DATABASE DBT_TEST
  AS PERMANENT = 60e6, SPOOL = 120e6;
.logoff
EOF

bteq < /tmp/pytestdatabase.bteq
