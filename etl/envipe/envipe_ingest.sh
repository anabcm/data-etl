for i in $(seq -f "%02g" 2017 2019)
do
    bamboo-cli --folder . --entry envipe_pipeline --year="$i"
done
