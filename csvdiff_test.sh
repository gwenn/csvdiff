#!/bin/sh
./csvdiff -k=1 testA.csv testB.csv

echo
echo "Test with gzipped files..."
gzip -c testA.csv > testA.csv.gz
gzip -c testB.csv > testB.csv.gz

./csvdiff -k=1 testA.csv.gz testB.csv.gz

rm testA.csv.gz
rm testB.csv.gz

echo
echo "Test with ignored field..."
./csvdiff -k=1 -i=2 testA.csv testB.csv
