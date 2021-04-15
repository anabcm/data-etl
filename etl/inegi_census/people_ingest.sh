#!/bin/bash

for i in $(seq -f "%02g" 1 32); 
do
for j in "2020";
do
    bamboo-cli --folder . --entry people_pipeline --index="$i"
done;
done;