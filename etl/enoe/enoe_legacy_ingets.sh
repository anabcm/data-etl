for i in $(seq -f "%02g" 10 20)
do
for j in $(seq -f "%01g" 1 4)
do
    bamboo-cli --folder . --entry enoe_pipeline_legacy --year="$i" --quarter="$j" 
done
done