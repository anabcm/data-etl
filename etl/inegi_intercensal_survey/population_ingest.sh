#!/bin/bash

for i in $(seq -f "%02g" 1 32)
do
    bamboo-cli --folder . --entry population_pipeline --index="$i" --source="population-data" --year="2015"
done;

for j in $(seq -f "%02g" 1 32)
do
    bamboo-cli --folder . --entry population_pipeline --index="$j" --source="population-data-2020" --year="2020"
done;