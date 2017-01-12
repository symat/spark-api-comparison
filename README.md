# spark-api-comparison
this repository created to contain example codes presented in a big data meetup: "Evolution of data modeling and optimizations in Spark (comparing RDD, DataFrame and DataSet APIs)"

# get the data

## easy way
 - download the data from here: https://gumicsizma.netbiol.org/data-imdb-2016-dec.tgz
 - extract it and fix the paths in the top of test class
 
## hard way
 - download the original data files from the IMDB ftp servers (see http://www.imdb.com/interfaces). You will need the following files:
    - actors.list.gz
    - actresses.list.gz
    - ratings-list-gz
 - use the parser scripts in the `data` folder on the downloaded files 
