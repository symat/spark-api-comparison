#!/bin/python3

import re
import json
import sys
import os


movie_ratings_pattern = re.compile(r"""(?P<distribution>[\d\.\*]{10}?)\t    # distribution of votes
                                       (?P<votes>[\d]*?)\t                  # number of votes
                                       (?P<rating>\d0?\.\d?)\t              # rating
                                       (?P<title>.+?)\t                     # movie title
                                       (?P<year>[\d]{4})""", re.VERBOSE)    # year

actor_pattern = re.compile(r"""(?P<actor>.+?)\t                     # distribution of votes
                               (?P<title>.+?)\t                     # movie title
                               (?P<year>[\d]{4})""", re.VERBOSE)    # year
							 							 
def extract_movie_ratings_data(s):
    try:
        return movie_ratings_pattern.search(s).groups()
    except AttributeError: 
        print("ERROR while parsing the following line from movie rating data file\n" + s)
    return None

def extract_actor_data(s):
    try:
        return actor_pattern.search(s).groups()
    except AttributeError: 
        print("ERROR while parsing the following line from actor data file\n" + s)
    return None


if len(sys.argv) != 4:
    print("ERROR\nusage: prepare_streaming_data.py <movie rating tsv file> <actor tsv file> <output folder>\n")
    sys.exit(1)





known_movies = set()
actor_data_by_year = dict()
movie_rating_data_by_year = dict()

movie_num = 0

for line in open(sys.argv[1], "r", encoding="utf-8"):
    if len(line.strip()) == 0:
        continue

    movie_data = extract_movie_ratings_data(line)
    if not movie_data:
        continue

    year = int(movie_data[4])
    title = movie_data[3]
    known_movies.add("%s_%d" % (title, year))

    movie_num += 1
    if year not in movie_rating_data_by_year:
        movie_rating_data_by_year[year] = []
    movie_rating_data_by_year[year].append(movie_data)

print("movies found: %d" % movie_num)


for year in movie_rating_data_by_year:
    out_file = open("%s/movie_ratings/movie_ratings_%d.tsv" % (sys.argv[3], year), "w", encoding="utf-8")
    for movie_data in movie_rating_data_by_year[year]:
        distribution_sum = 0
        for digit in range(10):
            distribution_sum += int(movie_data[0][digit])
        bucket_value = (int(movie_data[1]) / distribution_sum) / 1000
        for digit in range(10):
            number_of_votes = int(bucket_value * int(movie_data[0][digit]))
            to_print = "\t".join([str(digit+1), movie_data[3], movie_data[4]])
            for i in range(number_of_votes):
                print(to_print, file = out_file)
                print(to_print)
    out_file.close()


for actor_file in os.listdir(sys.argv[2]):
    for line in open("%s/%s" % (sys.argv[2], actor_file), "r", encoding="utf-8"):
        if len(line.strip()) == 0:
            continue
    
        actor_data = extract_actor_data(line)
        if not actor_data:
            continue

        year = int(actor_data[2])        
        title = actor_data[1]
        if "%s_%d" % (title, year) in known_movies:
            if year not in actor_data_by_year:
                actor_data_by_year[year] = []
            actor_data_by_year[year].append(actor_data)

for year in actor_data_by_year:
    out_file = open("%s/actor_data/actor_data_%d.tsv" % (sys.argv[3], year), "w", encoding="utf-8")
    for actor_data in actor_data_by_year[year]:
        to_print = "\t".join(actor_data)
        print(to_print, file = out_file)
        print(to_print)
    out_file.close()
