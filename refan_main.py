###################
# PyCon 2018 Project Submission
# "Visualizing Global Refugee Crisis using Pythonic ETL"
# yen.kha@ruralsourcing.com 

# The main module will compile using the following dependencies:
# refan_etl 
# geolatslons
# refan_plots 
###################

import refan_etl as re #module for ETL processes
import refan_plots as rp #module for data visualizations
import geolatslons as ge #module for geocoding
import csv 
# import urllib.request as ur
import pandas as pd 

##############
# Function call of the master dataset 
#############
# url = "https://raw.githubusercontent.com/yenk/Visualizing_Global_Refugee_Crisis_Using_Pythonic_ETL/master/unhcr_time_series_population.csv"
# filename = ur.urlopen("https://raw.githubusercontent.com/yenk/Visualizing_Global_Refugee_Crisis_Using_Pythonic_ETL/master/unhcr_time_series_population.csv")
# filename = ur.urlopen(url)
# filename = pd.read_csv('https://raw.githubusercontent.com/yenk/Visualizing_Global_Refugee_Crisis_Using_Pythonic_ETL/master/unhcr_time_series_population.csv',header=None, low_memory=False)
filename = 'unhcr_time_series_population.csv'
by_year = re.master_by_year(filename)

############
# Function calls to compile data and plot total 
# refugee population from 1952-2016
############
dict_allyears = re.popsum_allyears(by_year)
# rp.total_refugee_population(dict_allyears) 

############
# Function calls to compile and plot ten year 
# refugee population from 2007-2016 
#############
dict_year = re.popsum(by_year) 
# rp.total_10_year_refugee_population(dict_year) 

############
# Function calls to get top 10 countries with highest 
# refugee population from country_latslons_dict for 2007-2016
#############
input_latslons_dict = ge.input_country_latslons('country_latslons2.csv')
dict_country_count = re.yearcountry(by_year) 
year_country_top10_list = re.top_10_country_year(dict_country_count) 
top_10_country_latslons = re.top_10_country_map(year_country_top10_list, input_latslons_dict) 
# rp.country_resid_highest_pop(top_10_country_latslons) 

############
# Function calls to yield 10 year 
# comparison by population type 
#############
dict_poptype_year = re.poptypecount_byyear2(by_year)
# rp.ten_year_pop_type_comparison(dict_poptype_year) 

############
# Function calls to output total refugee 
# poulation by status over 10 year
#############
dict_poptype_count = re.populationtype_count(by_year)
# rp.total_pop_type_10_span(dict_poptype_count) 

#############
# Function calls for origin and country 
# of residence projection map 
#############
# Retrieving the top 10 countries with highest population types 
dict_poptype_country_count = re.poptypes_country_count(by_year)
country_poptype_list = re.top_10_poptype_country(dict_poptype_country_count)

# Retrieving the top 10 origins with highest population types 
dict_poptype_origin_count = re.poptypes_origin_count(by_year)
origin_poptype_list  = re.top_10_origin_poptype(dict_poptype_origin_count)

# Geocoding and plot
top_10_country_poptype_latslons = re.top_10_country_poptype_map(country_poptype_list, input_latslons_dict)
top_10_origin_poptype_latslons = re.top_10_origin_poptype_map(origin_poptype_list, input_latslons_dict)
# rp.country_origin_pop_types(top_10_country_poptype_latslons,top_10_origin_poptype_latslons) 




