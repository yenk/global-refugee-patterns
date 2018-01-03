###################
# PyCon 2018 Project Submission
# "Visualizing Global Refugee Crisis using Pythonic ETL"
# yen.kha@ruralsourcing.com 
###################

import csv
import refan_plots as rp #data visualization module 
import pandas as pd 

from collections import defaultdict

###################
# ETL processes
###################

######
# str is the parameter used for converting missing values 
# from a string to an integer to handle error exceptions
######
def convert(str):
  try: 
    return int(str)
  except ValueError: 
    return 0

######
# filename corresponds to the raw CSV file defined in refan_main.py
######
def master_by_year(filename):
  by_year = []

  with open(filename) as file: 
    reader = csv.reader(file)
    header_row = next(reader)
    
    for row in reader: 
      year = convert(row[0])
      country = row[1]
      origin = row[2]
      poptype = row[3]
      popvalue = convert(row[4])    
      new_row = dict({'Year':year,'Country':country,'Origin':origin,'Population Type':poptype,'Population Count':popvalue})
      by_year.append(new_row)
  return by_year 

######
# Exploring unique observations using  
# Panda's method calls: apply(), iteritems()  
######
df = pd.read_csv('https://raw.githubusercontent.com/yenk/Visualizing_Global_Refugee_Crisis_Using_Pythonic_ETL/master/unhcr_time_series_population.csv',header=None, low_memory=False)
# print(list(df.apply(set)[3])) 

# for i, item in df.iteritems(): 
    # print(item.unique())

######
# ETL to retrieve dataset by year from 1952-2016
######
def popsum_allyears(by_year):
  dict_allyears = {}

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']

      if year in dict_allyears :
        dict_allyears [year]+= count
      else: 
        dict_allyears [year] = count
  return dict_allyears 

######
# ETL to get the total population by year from 2007-2016 
######
def popsum(by_year):
  dict_year = {}

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']

      if year >= 2007: 
        if year in dict_year:
          dict_year[year]+= count
        else: 
          dict_year[year] = count
  return dict_year

###################
# Country analysis 
###################

######
# ETL to extract aggregated data based on total 
# refugee population by year and country 
# by_year is a list of transformed data strings used for data aggregation
######
def yearcountry(by_year):
  dict_country_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']

    if year >= 2007: 
        if country in dict_country_count: 
          dict_country_count[country]+= count 
        else: 
          dict_country_count[country] = count 
  return dict_country_count

######
# dict_country_count is a dictionary where countries are being 
# extracted into a list
######
def get_country_list(dict_country_count): 
  
  year_country_list = [] 

  for country_key in sorted(dict_country_count): 
    year_country_list.append(country_key)
  return year_country_list

######
# dict_country_count is a dictionary used to output 
# top 10 countries with highest refugee population
######
def top_10_country_year(dict_country_count):
  return sorted(dict_country_count, key=dict_country_count.get, reverse=True)[:10]
  #iterates through the dict, then gets key/value
  # return sorted(dict_country_count.items(), key = operator.itemgetter(1) , reverse = True)[:10] 

######
# year_country_top10_list is a list of the 10 countries as strings 
# country_latslons_dict is a dictionary of the 10 countries as namedtuples with coordinates 
######
def top_10_country_map(year_country_top10_list, country_latslons_dict):

  top_10_country_latslons = []
  for country in year_country_top10_list: 
    if country in country_latslons_dict: 
      top_10_country_latslons.append(country_latslons_dict[country])
  return top_10_country_latslons

###################
# Refugee analysis 
###################

######
# ETL to get population type and count by year
# by_year is a list of transformed data strings 
######
def poptypecount_byyear2(by_year):
  dict_poptype_year = {} 

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']
      poptype = row['Population Type']

      if year >= 2007: 
               
        if year in dict_poptype_year:
          poptype_dict = dict_poptype_year[year]
        else:
          poptype_dict = {} 
          dict_poptype_year[year] = poptype_dict
        if poptype in poptype_dict: 
          poptype_dict[poptype] += count 
        else: 
          poptype_dict[poptype] = count 
  return dict_poptype_year 

######
# ETL to get data for total population types and 
# count across 10 year from 2007-2016
# by_year is a list of transformed data strings 
######

def populationtype_count(by_year):
  dict_poptype_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    poptype = row['Population Type']

    if year >= 2007: 
        if poptype in dict_poptype_count: 
          dict_poptype_count[poptype] += count 
        else: 
          dict_poptype_count[poptype] = count 
  return dict_poptype_count 

######
# Alternative ETL using Defaultdict() Function to retrieve data 
# for total population types and count across 10 year from 2007-2016
# by_year is a list of transformed data strings 
######

def statuscount_byyear(by_year):
  
  dict_status_year = defaultdict(lambda : defaultdict(int)) #default value of int is 0

  for row in by_year: 
      year = row['Year']
      count = row['Population Count']
      status = row['Population Type']

      if year >= 2007:
          dict_status_year[year][status] += count
  return dict_status_year

######
# ETL to retrieve 3 year of origin's data for refugee types that have the highest 
# number of total population from 2014 - 2016 
# # by_year is a list of transformed data strings 
######
def poptypes_origin_count(by_year):
  dict_poptype_origin_count = {}

  for row in by_year: 
    year = row['Year']
    count = row['Population Count']
    poptype = row['Population Type']
    origin = row['Origin']

    if year >= 2014 and (poptype == 'Refugees(incl. refugee-like situations)' or poptype == 'Asylum-seekers'): 
        if origin in dict_poptype_origin_count :
          dict_poptype_origin_count[origin]+= count 
        else: 
          dict_poptype_origin_count[origin] = count 
  return dict_poptype_origin_count

######
# dict_poptype_origin_count is a dictionary where key is a string from origins 
# note: "various/unknowns" is listed as an origin and 
# are being ignored as it cannot be geocoded
# additional origin is retrieved for the next highest population 
######
def top_10_origin_poptype(dict_poptype_origin_count):
    return sorted(dict_poptype_origin_count, key=dict_poptype_origin_count.get, reverse=True)[:11] 

######
# ETL to retrieve 3 year of country of residence data for refugee types that have the highest 
# number of total population from 2014 - 2016 
# # by_year is a list of transformed data strings 
######
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

######
# dict_poptype_country_count is a dictionary where key is a string from counntry of residence 
######
def top_10_poptype_country(dict_poptype_country_count):
  return sorted(dict_poptype_country_count, key=dict_poptype_country_count.get, reverse=True)[:10] 

######
# ETL to retrieve lats and lons for refugees' country of residence 
# country_poptype_list is a list of top ten countries as strings 
# poptype_latslons_dict is a dictionary read from country_latslons2.csv 
######
def top_10_country_poptype_map(country_poptype_list, poptype_latslons_dict):
  top_10_country_poptype_latslons = []

  for country_poptype in country_poptype_list: 
    if country_poptype in poptype_latslons_dict: 
      top_10_country_poptype_latslons.append(poptype_latslons_dict[country_poptype])
  return  top_10_country_poptype_latslons

######
# ETL to retrieve lats and lons on refugee's origin 
# origin_poptype_list is a list of top ten origins as strings 
# poptype_latslons_dict is a dictionary read from country_latslons2.csv 
######
def top_10_origin_poptype_map(origin_poptype_list, poptype_latslons_dict):
  top_10_origin_poptype_latslons = []

  for origin_poptype in origin_poptype_list: 
    if origin_poptype in poptype_latslons_dict: 
      top_10_origin_poptype_latslons.append(poptype_latslons_dict[origin_poptype])
  return top_10_origin_poptype_latslons



