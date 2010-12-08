#!/bin/sh

for i in 1 2 3 4
do

	cat $1 >> $2
	echo " " >> $2
done
