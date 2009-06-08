#!/bin/sh

./Test.csh 2>&1 | tee Test.log

# Save test start time
START_TIME=$(date +%s)

# Execute all tests

./Test-loader.csh 2>&1 | tee Test-loader.log

# Save test end time
END_TIME=$(date +%s)

DIFF_TIME=$(( $END_TIME - $START_TIME ))

echo "Tests execution time is $DIFF_TIME seconds." > exectime.txt

echo
echo
cat exectime.txt
echo
echo
