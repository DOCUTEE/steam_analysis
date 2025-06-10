#!/bin/bash

schematool -dbType mysql -initSchema -verbose || true

hive --service metastore