for i in $(seq -f "%02g" 1 32)
do
    bamboo-cli --folder . --entry housing_pipeline_2020 --index="$i"
done