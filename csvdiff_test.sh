#!/bin/sh
./csvdiff -b -s=, -k=1 testA.csv testB.csv

echo
echo "Test with gzipped files..."
gzip -c testA.csv > testA.csv.gz
gzip -c testB.csv > testB.csv.gz

./csvdiff -b -s=, -k=1 testA.csv.gz testB.csv.gz

rm testA.csv.gz
rm testB.csv.gz
