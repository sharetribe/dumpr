#!/bin/bash

set -e

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y default-mysql-client
