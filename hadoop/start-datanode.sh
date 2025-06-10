#!/bin/bash

# Start SSH
service ssh start

# Hold the container open
tail -f /dev/null
