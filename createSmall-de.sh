#!/usr/bin/env bash

#stop at first error, unset variables are errors
set -o nounset
set -o errexit

if [ "$#" -ne 1 ]; then
    echo "Please supply a single argument, the directory to save the dataset"
    exit 1
fi

./create.sh $1 configSmall-de.properties
