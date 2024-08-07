mkdir -p dataset

rm -f ./dataset/*
wget -nc https://datasets.imdbws.com/title.ratings.tsv.gz -P ./dataset/
gunzip './dataset/title.ratings.tsv.gz'

wget -nc https://datasets.imdbws.com/title.basics.tsv.gz -P ./dataset/
gunzip './dataset/title.basics.tsv.gz'

wget -nc https://datasets.imdbws.com/name.basics.tsv.gz -P ./dataset/
gunzip './dataset/name.basics.tsv.gz'
