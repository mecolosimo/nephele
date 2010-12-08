#!/bin/sh

for i in `ls ~/panjo/dta/test_mats/*.mat`; 
do
    for j in 2 4 5 10 20 40;
    do
      mpirun -np $j ~/panjo/src/panjo -f $i >> ~/panjo/dta/test_mats/run_times.out 
      sleep 2
    done
done


