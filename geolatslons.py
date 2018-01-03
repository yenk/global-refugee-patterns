###################
# PyCon 2018 Project Submission
# "Visualizing Global Refugee Crisis using Pythonic ETL"
# yen.kha@ruralsourcing.com 
###################

from collections import namedtuple
from geopy.geocoders import Nominatim #openstreetmap API library 
from geopy.exc import GeocoderTimedOut #manipulate timeout when geocoding compiles

import csv 

###################
# Geocoding library
# certain countries that cannot be geocoded or yield wrong coordinates 
# are corrected on csv_outfile including China, Dem. Rep. of the Congo, 
# Eritrea, Iran (Islamic Rep. of), Malaysia, Myanmar, 
# Syrian Arab Rep., and United Kingdom 
###################

# Creating a namedtuple class for geocoding
latslons = namedtuple('latslons',['lat', 'lon']) 

######
# country_list is list of country names as strings 
# countries that cannot be geocoded are skipped 
######
def geos_country(country_list): 
    #initializing nomatim server with a higher timeout 
    geolocator=Nominatim(timeout=10)

    geo_country_dict = {}
    for country_key in country_list:
        location = geolocator.geocode(country_key)
        if location: 
          geos = latslons(location.latitude, location.longitude)     
          geo_country_dict[country_key] = geos 
    return geo_country_dict 

###################
# Method to output geocodes to CSV
###################

######
#geo_country_dict is a dictionary of geocoded countries 
#csv_outfile is the output file from the geocoded dictionary
######
def output_country_latslons(geo_country_dict, csv_outfile):
  with open(csv_outfile,'w') as output: 
    fieldnames = ['country','lats','lons'] 
    write = csv.DictWriter(output, fieldnames=fieldnames)
    for key, value in geo_country_dict.items(): 
      write.writerow({'country': key, 'lats': value[0],'lons': value[1]}) 

###################
#Method to read geocodes from CSV
###################

######
#csv_infile is the name of the string file being read from geocoding 
######
def input_country_latslons(csv_infile): 
  input_latslons_dict = {}

  with open(csv_infile, 'r') as infile: 
    read = csv.reader(infile)
  # print(read)
    for row in read:
      country,lat,lon = row 
      input_latslons_dict[country] = latslons(float(lat),float(lon)) #namedtuple 
  return input_latslons_dict
