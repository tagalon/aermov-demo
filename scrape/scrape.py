import os, uuid
import re
import time
import json
import requests
import datetime
import configparser
from elasticsearch import Elasticsearch, helpers
from requests.auth import HTTPBasicAuth
from mappings_data import mappings


# response = requests.get('http://localhost:9200', auth = HTTPBasicAuth(username, password))
# print(response.content)
air_movie_data, apiKey = {}, 'd44cc6dc'

# AA invalid url, now have to scrape json page instead
AA_URL = "https://entertainment.aa.com/api/proxy/request?api=/film/&resource=buckets/69/films&lang=en&flightdate=&flightsystem=&routes=&services=&type=Movies&page=1&pageSize=600&orderBy=titleasc&subsId=&dubsId="
AirCanada_URL = "https://entertainment.aircanada.com/api/proxy/request?api=/film/&resource=buckets/47/films&lang=en&flightdate=&flightsystem=&routes=&services=&type=Movies&page=1&pageSize=480&orderBy=yeardesc&subsId=&dubsId="
BritishAirways_URL = "https://entertainment.ba.com/api/proxy/request?api=/film/&resource=buckets/68/films&lang=en&flightdate=&flightsystem=&routes=&services=&type=Undefined&page=1&pageSize=60&orderBy=titleasc&subsId=&dubsId="
URL_List = [AirCanada_URL, BritishAirways_URL]
URL_Properties = ["AA", "AC", "BA"]
# Calling OMDB API for movie ratings/reviews and correspondly appending info to the matched movie
DeltaAirlines_URL = "https://www.delta.com/us/en/onboard/inflight-entertainment/current-movies"

# Needs Selenium
EmiratesAirlines_URL = "https://www.emirates.com/service/exp/ice?date=2023-05-18&group=movies"
AlaskAirlines_URL = "https://api.themoviedb.org/4/list/104189?api_key=a1b011e727b03fda91b225838563b27b&sort_by=title.asc&language=en-US&page=1"
#JetBlue uses Amazon, need to do more research about that flight

SouthwestAirlines_URL= "https://www.southwest.com/inflight-entertainment-portal/"
 
def filterMovieTitle(title):
    # a = ['Your Turn to Kill: The Movie\n' , 'Frozen (2013)', 'Valeria Mithatenet (Valeria is Getting Married)\n']
    # Need to figure out proper regex expression to filter out text
    if '\n' in title:
        title = title[:len(title)-1]
    if "(" in title and ")" in title:
        title = re.sub("\(.*?\)","()", title)
        arrtitle = title.split()
        arrtitle = arrtitle[0:len(arrtitle) - 1]
        title = ' '.join(arrtitle)
    return title

movieTitles = {}
def multiscrapURL(urls, props):
    for i in range(len(urls)):
        f = requests.get(urls[i])
        # rawMovieData = json.loads(f.text)
        rawMovieData = f.json()
        for movie in rawMovieData["data"]:
            movieTitle = filterMovieTitle(movie["title"])
            keyMovieInfo = (movieTitle, str(movie["year"]))
            if keyMovieInfo in movieTitles:
                if props[i][0] == ' ':
                    props[i] = props[i][1:]
                movieTitles[keyMovieInfo].append(props[i])
            else:
                movieTitles[keyMovieInfo] = [props[i]]
    print(movieTitles) 
    return movieTitles

def multiParameterGen(args):
    if len(args) == 1:
        return {"Source": args}
    gen = []
    # Actors, Airline, Genre -> [a, b, c] -> [{'Name': a}, {'Name': b}, {'Name': c}] 
    for arg in args:
        gen.append({"Source": arg})
    return gen

def checkMultipleGenres(query):

    if " " in query["Genre"]:
        query["Genre"] = query["Genre"].split(',')
    else:
        query["Genre"] = [query["Genre"]]
    return query

def scrapeMovies():
    failedTitles = []
    movieProps = multiscrapURL(URL_List, URL_Properties)

    for movie in movieProps:
        movieTitle, year, props = movie[0], movie[1], movieTitles[movie]
        # movieTitle, year = movieInfo[0], movieInfo[1]
        data_URL = 'http://www.omdbapi.com/?t='+movieTitle+'&y='+year+'&apikey='+apiKey
        metaQuery = requests.get(data_URL).json()
        if metaQuery["Response"] == "True":
            metaQuery['Airline'] = props
            air_movie_data[metaQuery["Title"]] = checkMultipleGenres(metaQuery)
        elif metaQuery["Response"] == "False":
            failedTitles.append((movieTitle, year))
    return failedTitles

def loadJSONMovies():

    # Setting up ElasticSearch Configuration to API
    config = configparser.ConfigParser()
    config.read('example.ini')
    es = Elasticsearch(cloud_id=config['ELASTIC']['cloud_id'], basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password']))


    f = open('old_aa_data.json')
    
    data = json.load(f)
    def ingestRatings(arg):
        for doc in data:
            ratings = ["Rotten Tomatoes", "Metacritic", "Internet Movie Database"]
            for source in data[doc]['Ratings']:
                k, v = source.values()
                ratings.remove(k)
                
                if "%" not in v:
                    arr = v.split("/")
                    value = str(int(float(arr[0]) / float(arr[1]) * 100)) + "%"
                    print(value)
                    data[doc][k] = value
                else:
                    data[doc][k] = v
            if ratings:
                for rating in ratings:
                    data[doc][rating] = "NA"
            data[doc].pop("Ratings")
            data[doc]["Director"] = data[doc]["Director"].split(',')
            data[doc]["Writer"] = data[doc]["Writer"].split(',')
            data[doc]["Writer"] = data[doc]["Actors"].split(',')
        return arg 
    data = ingestRatings(data)

    def generate_actions():
        return [{
        '_source': {
            'Title': data[doc]['Title'],
            'Year': data[doc]['Year'],
            'Rated': data[doc]['Rated'],
            'Released': data[doc]['Released'],
            'Runtime': data[doc]['Runtime'],
            'Genre': data[doc]['Genre'],
            'Director': data[doc]['Director'],
            'Writer': data[doc]['Writer'],
            'Actors': data[doc]['Actors'],
            'Plot': data[doc]['Plot'],
            'Language': data[doc]['Language'],
            'Country': data[doc]['Country'],
            'Awards': data[doc]['Awards'],
            'Poster': data[doc]['Poster'],
            'Internet Movie Database': data[doc]['Internet Movie Database'],
            'Rotten Tomatoes': data[doc]['Rotten Tomatoes'],
            'Metacritic': data[doc]['Metacritic'],
            'imdbRating': data[doc]['imdbRating'],
            'imdbVotes': data[doc]['imdbVotes'],
            'imdbID': data[doc]['imdbID'],
            'Type': data[doc]['Type'],
            'BoxOffice': data[doc]['BoxOffice'],
            'Airline': data[doc]['Airline'],
            '@timestamp': str(datetime.datetime.utcnow().strftime("%Y-%m-%d"'T'"%H:%M:%S")),}
        }
        for doc in data]
    
    es.indices.put_mapping(body=mappings, index='search-movies', ignore=400)
    data = generate_actions()
    for title in data:
        print(title)
    try:
    # make the bulk call, and get a response
        response = helpers.bulk(es, index = "search-movies", actions = data)
        print ("\nRESPONSE:", response)
    except Exception as e:
        print("\nERROR:", e)

    # try:
    # # create JSON string of doc _source data
    #     json_source = json.dumps(build_doc["source"])

    # # get the dict object's _id
    #     json_id = build_doc["id"]
    #     build_doc["source"] = json_source
    # # make an API call to the Elasticsearch cluster
    #     response = es.index(
    #         #Index name in Kibana
    #         index = 'search-am',
    #         document = json_source
    #     )
    #     print
    #     # print a pretty response to the index() method call response
    #     print ("\nclient.index response:", json.dumps(response, indent=4))
    # except Exception as error:
    #     print ("Error type:", type(error))
    #     print ("client.index() ERROR:", error)
    # print(es.info)

    # print ("\nbuild_doc items:", build_doc.items())

    # es.index(index="my-index-000001", document=air_movie_data.values())
    # helpers.bulk(es, json_str)

    # all_docs = {}
    # all_docs["size"] = 9999
    # all_docs["query"] = {"match_all" : {}}
    # print ("\nall_docs:", all_docs)
    # print ("all_docs TYPE:", type(all_docs))


    # try:
    #     # pass the JSON string in an API call to the Elasticsearch cluster
    #     response = es.search(
    #     index = "some_index",
    #     body = all_docs
    #     )

    #     # print all of the documents in the Elasticsearch index
    #     print ("all_docs query response:", response)

    #     # use the dumps() method's 'indent' parameter to print a pretty response
    #     print ("all_docs pretty:", json.dumps(response, indent=4))

    # except Exception as error:
    #     print ("Error type:", type(error))
    #     print ("client.search() ValueError for JSON object:", error)

    #Testing for proper JSON Format

# Ingests movies into ElasticSearch


# failCount = scrapeMovies()
# print("Total Movie Count:" + str(len(movieTitles)))
# print("Total Filtered:" + str(len(air_movie_data)))
loadJSONMovies()
# Elasticsearch

