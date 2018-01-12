#!/bin/bash

models=( "en-token.bin" "en-sent.bin" "en-pos-maxent.bin" "en-chunker.bin" "en-parser-chunking.bin" )
nermodels=( "en-ner-date.bin" "en-ner-location.bin" "en-ner-money.bin" "en-ner-organization.bin" "en-ner-percentage.bin" "en-ner-person.bin" "en-ner-time.bin" )

allmodels=("${models[@]}" "${nermodels[@]}")

#download the models from http://opennlp.sourceforge.net/models-1.5/
for file in "${allmodels[@]}"
do
	if [ ! -f $file ]; then
		wget http://opennlp.sourceforge.net/models-1.5/$file
	fi
done


#CONSIDER: instead use download-maven-plugin (https://stackoverflow.com/questions/2741806/maven-downloading-files-from-url) in the nlp project
# run in the validate phase (first phase)
# to download each OpenNLP model file to ${project.basedir}/src/main/resources
