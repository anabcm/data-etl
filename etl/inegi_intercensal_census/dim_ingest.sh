for i in $(seq -f "%02g" 1 32)
do
    bamboo-cli --folder etl/inegi_intercensal_census --entry dim_geography_pipeline --index="$i"
done

bamboo-cli --folder etl/inegi_intercensal_census --entry dim_sex_pipeline
