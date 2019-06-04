for i in $(seq -f "%02g" 1 32)
do
    bamboo-cli --folder etl/inegi_intercensal_census --entry population_pipeline --index="$i"
done
