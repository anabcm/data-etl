#!/bin/bash

for i in $(seq -f "%02g" 1 32); 
do
for j in "2010" "2020";
do
    bamboo-cli --folder . --entry population_pipeline_2010 --index="$i" --year="$j"
done;
done;
