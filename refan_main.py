"""
PyCon 2018 Project Submission
"Visualizing Global Refugee Crisis using Pythonic ETL"
yen.kha@ruralsourcing.com 

This main module will execute refan_etl and refan_plots. 

"""

import refan_etl as re 
import refan_plots as rp 


#Reading CSV and writing into a dictionary for mapping
country_latslons_dict = {}

with open('country_latslons2.csv', 'r') as infile: 
  read = csv.reader(infile)
# print(read)
  for row in read:
    country,lat,lon = row 
    country_latslons_dict[country] = latslons(float(lat),float(lon)) #namedtuple 
# print(country_latslons_dict)

dict_allyears = popsum_allyears(by_year) #function call to output data for plotting
rp.total_refugee_population(dict_allyears) #calling data visualization module 

############
#Function calls to compile ten year refugee population by year beginning 2007-2016 
#############

dict_year = popsum(by_year) 
rp.total_10_year_refugee_population(dict_year) #plot ten year refugee population 

############
#Function calls to get top 10 countries with highest refugee population from CSV
#############
year_country_list = top_10_country_year(dict_country_count)

top_10_country_latslons = top_10_country_map(year_country_list, country_latslons_dict) #calling country_latslons2 csv file
rp.country_resid_highest_pop(top_10_country_latslons) #plot countries with highest refugee population 

############
#Function calls to yield 10 year comparison by population type 
#############

dict_poptype_year = poptypecount_byyear2(by_year)
rp.ten_year_pop_type_comparison(dict_poptype_year) #plot 10 year population type comparison


############
#Function calls to output total refugee poulation by status over 10 year
#############

dict_poptype_count = populationtype_count(by_year)
rp.total_pop_type_10_span(dict_poptype_count) #pot total refugee population by status over 10 year 


############
#Function calls for origin and country of residence projection map 
#############

#function pertains to country lats/lons
top_10_country_poptype_latslons = top_10_country_poptype_map(country_poptype_list, poptype_latslons_dict)

#function pertains to origin lats/lons
top_10_origin_poptype_latslons = top_10_origin_poptype_map(origin_poptype_list, poptype_latslons_dict)

#plot origin and country of residence by refugee populations 
rp.country_origin_pop_types(top_10_country_poptype_latslons,top_10_origin_poptype_latslons) 

