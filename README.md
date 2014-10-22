ML with Spark via MLlib
================
#####Update
Added the MovieLensALS Recommender, using the movie lens dataset.
download the dataset first, for that run the script ./movielens.sh

You have three sizes of dataset available. Since this is CPU heavy
 it can take a lot of time especially if run on a laptop. Try with 4 
cores --master local[4]
Or just take either the smallest dataset(5M) or the mid size one with 1M ratings
wget http://files.grouplens.org/datasets/movielens/ml-1m.zip
unzip ....

and run it spark-submit --class sparkapps.MovieLensALS --master local .....

Read the explanation .. And ideally modify the code to send all the 
outputs(println) to a file that could be read later ....

This is an example from a spark training done by the berkeley guys ...
The full tutorial with explanation is here.
http://ampcamp.berkeley.edu/4/exercises/movie-recommendation-with-mllib.html
