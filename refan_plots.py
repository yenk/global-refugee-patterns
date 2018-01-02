"""
PyCon 2018 Project Submission
"Visualizing Global Refugee Crisis using Pythonic ETL"
yen.kha@ruralsourcing.com 

This module creates all the data visualizations using outputs 
from refan_etl. 

"""

import pandas as pd 
import numpy as np 

from matplotlib import pyplot as plt
from mpl_toolkits.basemap import Basemap

############
#PLOTTING: bar chart for total population across all years: 1952-2016
###########

def total_refugee_population(dict_allyears):

  plt.bar(range(len(dict_allyears)), dict_allyears.values(), align="center", color='#EE3224')
  plt.xticks(range(len(dict_allyears)), list(dict_allyears.keys()), rotation=90)

  plt.title('Total Refugee Population: 1952-2016',fontweight='bold', color='g', fontsize='12')
  plt.xlabel('By Year', fontweight='bold', color='g', fontsize='10')
  plt.ylabel('Total Population Size in Millions',fontweight='bold', color='g', fontsize='10')
  plt.grid(True)
  plt.show()

##################
#PLOTTING: bar chart - total refugee population for 2007-2016
###################

#creating a bar graph from matplotlib by year and total population

def total_10_year_refugee_population(dict_year):

  plt.bar(range(len(dict_year)), dict_year.values(), align="center", color='#EE3224')
  plt.xticks(range(len(dict_year)), list(dict_year.keys()), rotation=90)
  plt.grid(True)

  plt.title('Total Refugee Population: 2007-2016',fontweight='bold', color='g', fontsize='12')
  plt.xlabel('By Year', fontweight='bold', color='g', fontsize='10')
  plt.ylabel('Total Population Size in Millions',fontweight='bold', color='g', fontsize='10')
  plt.show()

  #saving plot 
  # plt.savefig('popsum_byallyear.png')

########################
#PROJECTION MAPPING: Countries of asylum-seeking population 
########################

#Loop through lats/lons csv to genereate (x,y) lists for reading.

def country_resid_highest_pop(top_10_country_latslons):

  lats,lons = [],[]

  for row in top_10_country_latslons: 
      # print(row)
    lats.append(row[0])
    lons.append(row[1])
  # print(lats,lons)

  #mapping lats/lons 
    country_map = Basemap(projection='moll', resolution = 'c', area_thresh=500.0,
        lat_0=0, lon_0=50)

    #drawing coastlines and country boundaries on the map
    country_map.drawcoastlines()
    country_map.drawcountries()
    country_map.fillcontinents(color='beige', lake_color='lightblue')
    country_map.drawmapboundary(fill_color='lightblue')

    #defining lats and lons lines: begin, end, apart from np.arrange() 
    country_map.drawmeridians(np.arange(0, 420, 60),color='beige', dashes=[1,3])
    country_map.drawparallels(np.arange(-90, 120, 60),color='beige', dashes=[1,3])

    #countries of asylum
    x,y = country_map(lons,lats)
    country_map.plot(x, y, 'g^', color='blue', markersize=6)

    plt.title('Country of Residence With Highest Total Population From All Refugee Categories: 2007-2016') 
    plt.show()

##########################
#PLOTTING: Using Panda's from_dict() object method 
#########################

def ten_year_pop_type_comparison(dict_poptype_year):

  df = pd.DataFrame.from_dict(dict_poptype_year, orient='columns', dtype=None)
  df.plot(kind='bar', stacked=False)

  plt.title('Total Population Type Comparison Across 10 Year Span: 2007-2016')
  plt.ylabel('Population Type in Millions')
  # plt.savefig('refugee_status_plot.png')
  plt.show()

#############################
#PLOTTING: Generate a scatter plot to represent 
#total refugee population across 10 year span: 2007 - 2016
############################

def total_pop_type_10_span(dict_poptype_count):

  #creating lists to generate a scatter plot from a dictionary
  poptype_data = list(dict_poptype_count.values())
  pop_types = list(dict_poptype_count.keys())

  #initializing axes instance
  fix, ax = plt.subplots()
  plt.plot(poptype_data, 'g^', linewidth=3, color='g') #creates a scatter plot

  # plt.plot(poptype_data,'r--', color='r') #creates a line plot 

  #retrieving text labels for plotting 
  labels = ['Internally Displaced','Returned IDPs','Asylum-seekers','Refugees(incl. refugee-like situations','Returnees','Stateless','Others of concern']

  x1 = [0,1,2,3,4,5,6]
  ax.set_xticks(x1)
  ax.set_xticklabels(labels, rotation='vertical')

  # plt.ylabel('Population Type in Millions')
  plt.title('Total Population Type Across Ten Year Span: 2007-2016')
  plt.grid(True)

  plt.show()
# plt.savefig('total_poptypes_count.png')

#####################
#PROJECTION MAPPING: Country and origin by population types 
#####################

def country_origin_pop_types(top_10_country_poptype_latslons,top_10_origin_poptype_latslons):

  #Create lists of lats/lons for country 
  clat,clon = [],[]

  for row in top_10_country_poptype_latslons: 
    # print(row)
    clat.append(row[0])
    clon.append(row[1])
  # print(clat,clon)

  #Create lists of lats/lons for country 
  olat,olon = [],[]

  for row in top_10_origin_poptype_latslons: 
    # print(row)
    olat.append(row[0])
    olon.append(row[1])
  # # print(olat,olon)

  # # #defining the map 
  poptype_map = Basemap(projection='moll', resolution = 'c', area_thresh=500.0,
      lat_0=0, lon_0=50)

  #drawing coastlines and country boundaries 
  poptype_map.drawcoastlines()
  poptype_map.drawcountries()
  poptype_map.fillcontinents(color='beige', lake_color='lightblue')
  poptype_map.drawmapboundary(fill_color='lightblue')

  #defining lats and lons lines: begin, end, apart from np.arrange() 
  poptype_map.drawmeridians(np.arange(0, 420, 60),color='beige', dashes=[1,3])
  poptype_map.drawparallels(np.arange(-90, 120, 60),color='beige', dashes=[1,3])

  # create plot axes for country and origin
  x,y = poptype_map(clon,clat)
  a,b = poptype_map(olon,olat)

  linexy,=poptype_map.plot(x, y, '*', color='red', markersize=6,label='Residence by Population Types')
  lineab,=poptype_map.plot(a, b, 'g^', color='green', markersize=6,label='Origin by Population Types')

  plt.legend(loc='upper center', bbox_to_anchor=(0.5,-0.05),ncol=5,fancybox=True,shadow=True)
  plt.title('Top Ten Global Refugee Populations Based on Refugee(Incl. Refugee-Like Situations) and Asylum-Seeker Types')
  plt.show()

# # plt.savefig('TopTenRefugeePop_ResidenceOrigins')




