#!/bin/bash

#stop at first error, unset variables are errors
set -o nounset
set -o errexit

scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Base directory to save the cc-dbp dataset in
baseDir=$1
# Configuration file to use
config=${2:-unaryConfig.properties}

# baseline context set construction
java -Xmx8G -cp com.ibm.research.ai.ki.kbp/target/kbp-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
com.ibm.research.ai.ki.kbp.KBPBuildDataset -unaryConfig $config -in $baseDir/docs-gaz.json.gz.b64 -out $baseDir/dataset -kb $baseDir/kb

# show sample of positive context sets
awk  -F $'\t' '$6!=""' $baseDir/dataset/unaryContextSets/contexts-part0.tsv | head
