import numpy as np
import pandas as pd
import shapefile as shp
import matplotlib.pyplot as plt
import seaborn as sns
import os

sns.set(style="whitegrid", palette="pastel", color_codes=True)
sns.mpl.rc("figure", figsize=(10,6))

#opening the vector map
shp_path = "./NILZone_U00-32.shp"
#reading the shape file by using reader function of the shape lib
sf = shp.Reader(shp_path)

len(sf.shapes())
sf.records()
