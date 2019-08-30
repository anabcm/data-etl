for i in $(seq -f "%04g" 2012 2017)
do
    bamboo-cli --folder . --entry emergency_pipeline --year="$i"
done