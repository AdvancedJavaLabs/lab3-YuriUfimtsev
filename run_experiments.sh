#!/bin/bash
# Всего данных ~ 132 MB

# actual splits numbers = 66,
SPLITS_MB=(1 2 4 8 16 32 64 140)
REDUCERS_COUNT=(1 2 4 8 16)

for s in "${SPLITS_MB[@]}"; do
  for r in "${REDUCERS_COUNT[@]}"; do
    echo "=== Running split=$s reducers=$r ==="
    ./run_pipeline.sh $s $r
    echo ""
  done
done

echo "Experiments completed. View report in ./reports/results.csv"
