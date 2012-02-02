#!/bin/sh
$GOPATH/bin/csvdiff -k=1 testA.csv testB.csv

echo
echo "Test with gzipped files..."
gzip -c testA.csv > testA.csv.gz
gzip -c testB.csv > testB.csv.gz

$GOPATH/bin/csvdiff -k=1 testA.csv.gz testB.csv.gz

rm testA.csv.gz testB.csv.gz

echo
echo "Test with pipe separator..."
tr ',' '|' < testA.csv > testA.tsv
tr ',' '|' < testB.csv > testB.tsv
$GOPATH/bin/csvdiff -s=\| -k=1 -f=1 testA.tsv testB.tsv
rm testA.tsv testB.tsv

echo
echo "Test with ignored field..."
$GOPATH/bin/csvdiff -k=1 -i=2 testA.csv testB.csv

echo
echo "Test with guessed separator..."
tr ',' ';' < testA.csv > testA.dsv
tr ',' ';' < testB.csv > testB.dsv
$GOPATH/bin/csvdiff -k=1 testA.dsv testB.dsv
rm testA.dsv testB.dsv
