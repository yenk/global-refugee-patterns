

"""
PyCon 2018 Project Submission
"Visualizing Global Refugee Crisis using Pythonic ETL"
yen.kha@ruralsourcing.com 

This module is created for ETL batch processes to do the following 
using python built-in data structures: 

->data extraction from UNHRC CSV dataset 
->data reading/loading/output from geocoding into CSV
->data transformation for visualizations referencing refan_plots. 

"""

import csv
import refan_plots as rp #data visualization module 
import pandas as pd 

from geopy.geocoders import Nominatim #openstreetmap API library 
from geopy.exc import GeocoderTimedOut #manipulate timeout when geocoding compiles

from collections import defaultdict
from collections import namedtuple


##################################################
# Initial Data Extraction/Transformation/Loading (ETL)
##################################################

filename = 'unhcr_time_series_population.csv'

def convert(str):
  '''
  Converts missing value from string to int 
  to handle error exceptions.
  '''
  try: 
    return int(str)
  except ValueError: 
    return 0

by_year = [] #master list dataset

with open(filename) as file: 
  reader = csv.reader(file)
  header_row = next(reader) #returns next line in the file
# print(header_row) #['Year', 'Country / territory of asylum/residence', 'Origin', 'Population type', 'Value']

  for row in reader: 
   # by_year.append(row)
   # print(row)
    year = convert(row[0])
    country = row[1]
    origin = row[2]
    poptype = row[3]
    popvalue = convert(row[4])
    
    # new_row = [] 
    # new_row.extend((year, country, origin, poptype, popvalue)) #create new row
    
    #create a dictionary to reassign variable names 
    new_row = dict({'Year':year,'Country':country,'Origin':origin,'Population Type':poptype,'Population Count':popvalue})

    by_year.append(new_row) #store new list of all new rows 
# print(by_year)

################
#Exploring data uniqueness in Pandas coupled with built-in data structures 
################

#Using Pandas apply() method to identify unique population type  
df = pd.read_csv('https://raw.githubusercontent.com/yenk/Visualizing_Global_Refugee_Crisis_Using_Pythonic_ETL/master/unhcr_time_series_population.csv',header=None, low_memory=False)
# print(list(df.apply(set)[3])) #Population Type is at index[3]

#Alternatively, use dictionary iterator coupled with Pandas to loop through 
#each of the unique values based on its corresponding keys. 
# for i, item in df.iteritems(): 
    # print(item.unique())


#################################
#Compute Year and Total Population
##################################

#########
#ETL: Dictionary - Dataset for all years from 1952-2016
#########
def popsum_allyears(by_year):
  dict_allyears = {}

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']

      # if year >= 2007: 
        # print(year)
      if year in dict_allyears : #if 'year' as the key in dict
        dict_allyears [year]+= count
        # print(dict_year)
      else: 
        dict_allyears [year] = count #initializing the count 
        # print(dict_year)
  return dict_allyears 
# print(popsum_allyears(by_year))

############
#ETL: Dictionary - total population by year beginning 2007-2016 
#############
def popsum(by_year):
  dict_year = {}

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']

      if year >= 2007: 
        # print(year)
        if year in dict_year: #if 'year' as the key in dict
          dict_year[year]+= count
          # print(dict_year)
        else: 
          dict_year[year] = count #initializing the count 
        # print(dict_year)
  return dict_year
# print(popsum(by_year))

#####################################################################################
#                           COUNTRY ANALYSIS 
####################################################################################

#############
#ETL: Dictionary - Extract aggregated data based on total refugee population by year and country 
#############

def yearcountry(by_year):
  dict_country_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    country = row['Country']
#     origin = row['Origin']

    if year >= 2007: 
        if country in dict_country_count: 
          dict_country_count[country]+= count 
        else: 
          dict_country_count[country] = count 
  return dict_country_count
# print(yearcountry(by_year))

#Generate a list of countries for mapping
dict_country_count = yearcountry(by_year)

def get_country_list(dict_country_count): 
  
  year_country_list = [] 

  for country_key in sorted(dict_country_count): 
    year_country_list.append(country_key)
  return year_country_list
# print(get_country_list(dict_country_count))

#Gecodoing countries to get latitudes and longitudes
year_country_list = get_country_list(dict_country_count)

#defining a class using a factory function - mamedtuple 
latslons = namedtuple('latslons',['lat', 'lon']) 

# def geos_country(year_country_list): 

#     #initializing nomatim server with a higher timeout 
#     geolocator=Nominatim(timeout=10)
#     #create a new list to hold the namedtuples
#     geo_country_dict = {}

#     for country_key in year_country_list:
#     # for origin_key in sorted(dict_origin_count):
#         location = geolocator.geocode(country_key)
#         #if there is a location, output lats and lons into geos
#         if location: 
#             #instantiating class - latslons 
#           geos = latslons(location.latitude, location.longitude)   
#              #retrieving country as dict key and geos for values       
#           geo_country_dict[country_key] = geos 
#     return geo_country_dict 
# print(geos_country(year_country_list))

#Writing geocode to CSV
# geo_country_dict = geos_country(year_country_list)

# #output to CSV files for further analysis to prevent exceeding API calls
# with open('country_latslons2.csv','w') as outfile: 
#   fieldnames = ['country','lats','lons'] 
#   write = csv.DictWriter(outfile, fieldnames=fieldnames)
#   for key, value in geo_country_dict.items(): 
#     write.writerow({'country': key, 'lats': value[0],'lons': value[1]}) 


#Reading CSV and writing into a dictionary for mapping
country_latslons_dict = {}

with open('country_latslons2.csv', 'r') as infile: 
  read = csv.reader(infile)
# print(read)
  for row in read:
    country,lat,lon = row 
    country_latslons_dict[country] = latslons(float(lat),float(lon)) #namedtuple 
# print(country_latslons_dict)

################
#ANALYSIS: Get top 10 countries with highest refugee population from CSV
################
dict_country_count = yearcountry(by_year)

def top_10_country_year(dict_country_count):
  return sorted(dict_country_count, key=dict_country_count.get, reverse=True)[:10]
    # return sorted(dict_country_count.items(), key = operator.itemgetter(1) , reverse = True)[:10] #iterates through the dict, then gets key/value
# print(top_10_country_year(dict_country_count)) #list of 10 countries 

#Generate final dataset by calling two parameters to get 
#the top 10 countries with lats and lons by 
#creating a new list from top 10 country function.

year_country_list = top_10_country_year(dict_country_count)

def top_10_country_map(year_country_list, country_latslons_dict):

  top_10_country_latslons = []
  for country in year_country_list: 
    if country in country_latslons_dict: 
      top_10_country_latslons.append(country_latslons_dict[country])
  return top_10_country_latslons
# print(top_10_country_map(year_country_list, country_latslons_dict))

###################################################################
#                      REFUGEE ANALYSIS 
####################################################################

###################
#ETL: Nested Dictionary - Get population type and count by year
####################

def poptypecount_byyear2(by_year):
  dict_poptype_year = {} #master dictionary 
  for row in by_year: 
      year = row['Year']
      count = row['Population Count']
      poptype = row['Population Type']
      # print year, status, count

      if year >= 2007: 
               
        if year in dict_poptype_year:
          poptype_dict = dict_poptype_year[year]
        else:
          poptype_dict = {} #inner dictionary 
          dict_poptype_year[year] = poptype_dict
        if poptype in poptype_dict: 
          poptype_dict[poptype] += count 
        else: 
          poptype_dict[poptype] = count 
    # print (dict_status_year)
  return dict_poptype_year 
# print(poptypecount_byyear2(by_year))

#######################
#ETL: Dictionary - Extract data for total Population types and 
#count across 10 year period 2007-2016
######################

def populationtype_count(by_year):
  dict_poptype_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    poptype = row['Population Type']
#     origin = row['Origin']

    if year >= 2007: 
        if poptype in dict_poptype_count: 
          dict_poptype_count[poptype] += count 
        else: 
          dict_poptype_count[poptype] = count 
  return dict_poptype_count 
# print(populationtype_count(by_year))

#################
#Alternative ETL Defaultdict() Function: nested dictionary: Extract data for total Population types and 
#count across 10 year period 2007-2016
#############

def statuscount_byyear(by_year):
  
  dict_status_year = defaultdict(lambda : defaultdict(int)) #default value of int is 0

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']
      status = row['Population Type']

      if year >= 2007:
       # print year, status, count
          dict_status_year[year][status] += count
    # print(dict_status_year)
  return dict_status_year
# print(statuscount_byyear(by_year))

################################
#ETL for Population Types: Dictionary - Get country 
#and origins by two highest population types for the last 3 years: 2014 - 2016 
#################################

#Getting origins based on refugee population types
def poptypes_origin_count(by_year):
  dict_poptype_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    poptype = row['Population Type']
    origin = row['Origin']

    if year >= 2014 and (poptype == 'Refugees(incl. refugee-like situations)' or poptype == 'Asylum-seekers'): 
        if origin in dict_poptype_count :
        # if origin in dict_poptype_count and (origin != 'Various/Unknown'): 
          dict_poptype_count[origin]+= count 
        else: 
          dict_poptype_count[origin] = count 
  return dict_poptype_count
# print(poptypes_origin_count(by_year))

#Retrieving the top 10 origins with highest population types 
dict_poptype_count = poptypes_origin_count(by_year)

def top_10_origin_poptype(dict_poptype_count):
    return sorted(dict_poptype_count, key=dict_poptype_count.get, reverse=True)[:11] #additional country added due to "various/unknowns" origin type 
    # return sorted(dict_poptype_count.items(), key = operator.itemgetter(1) , reverse = True)[:10] #gives country/count
# print(top_10_origin_poptype(dict_poptype_count))

#Getting countries based on refugee population types
def poptypes_country_count(by_year):
  dict_poptype_country_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    country = row['Country']
    poptype = row['Population Type']

    if year >= 2014 and (poptype == 'Refugees(incl. refugee-like situations)' or poptype == 'Asylum-seekers'): 
        if country in dict_poptype_country_count: 
          dict_poptype_country_count[country]+= count 
        else: 
          dict_poptype_country_count[country] = count 
  return dict_poptype_country_count
# print(poptypes_country_count(by_year))

#Retrieving the top 10 countries with highest population types 
dict_poptype_country_count = poptypes_country_count(by_year)

def top_10_poptype_country(dict_poptype_country_count):
  return sorted(dict_poptype_country_count, key=dict_poptype_country_count.get, reverse=True)[:10] 
    # return sorted(dict_poptype_count.items(), key = operator.itemgetter(1) , reverse = True)[:10] #itemgetter() method yields value/key pair

#Reading csv and writing into a dictionary using same latslons namedtuple from country analysis. 
poptype_latslons_dict = {}

with open('country_latslons2.csv', 'r') as infile: 
  read = csv.reader(infile)
  # print(infile)
  for row in read:
    # print(row)
    poptype,lat,lon = row 
    # print(row) 
    poptype_latslons_dict[poptype] = latslons(float(lat),float(lon)) #namedtuple data type conversion
  # print(country_poptype_latslons_dict)

#Create new lists to store top 10 countries and origins lats and lons 
country_poptype_list = top_10_poptype_country(dict_poptype_country_count)
origin_poptype_list  = top_10_origin_poptype(dict_poptype_count)

def top_10_country_poptype_map(country_poptype_list, poptype_latslons_dict):

  top_10_country_poptype_latslons = []
  '''
  retrieving lats and lons for country refugees 
  '''
  for country_poptype in country_poptype_list: 
    if country_poptype in poptype_latslons_dict: 
      top_10_country_poptype_latslons.append(poptype_latslons_dict[country_poptype])
  return  top_10_country_poptype_latslons
# print(top_10_country_poptype_map(country_poptype_list, poptype_latslons_dict))

def top_10_origin_poptype_map(origin_poptype_list, poptype_latslons_dict):

  top_10_origin_poptype_latslons = []
  '''
  retrieving lats and lons for origin refugees
  '''
  for origin_poptype in origin_poptype_list: 
    if origin_poptype in poptype_latslons_dict: 
      top_10_origin_poptype_latslons.append(poptype_latslons_dict[origin_poptype])
  return top_10_origin_poptype_latslons

# print(top_10_origin_poptype_map(origin_poptype_list, poptype_latslons_dict))

#####################
#Combining origin and country lats and lons  
#####################
# def top_10_poptype_map(country_poptype_list, origin_poptype_list,poptype_latslons_dict):

#   top_10_country_poptype_latslons = []
#   top_10_origin_poptype_latslons = []
#   '''
#   retrieving lats and lons for country refugees 
#   '''
#   for country_poptype in country_poptype_list: 
#     if country_poptype in poptype_latslons_dict: 
#       top_10_country_poptype_latslons.append(poptype_latslons_dict[country_poptype])
#   '''
#   retrieving lats and lons for origin refugees 
#   '''
#   for origin_poptype in origin_poptype_list: 
#     if origin_poptype in poptype_latslons_dict: 
#       top_10_origin_poptype_latslons.append(poptype_latslons_dict[origin_poptype])
#   return(country_poptype_list, origin_poptype_list,poptype_latslons_dict)
# print(top_10_poptype_map(country_poptype_list, origin_poptype_list,poptype_latslons_dict))


