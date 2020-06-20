import pandas as pd
from collections import defaultdict
import json
import folium
import re
from folium.plugins import MarkerCluster
from src.data_fetcher.caida_ixp_data import CaidaIxp
from src.data_cleaner.caida_ixp_cleaner import caidaIxpCleaner
from src.data_cleaner.ixp_location_cleaner import IxpLocationCleaner
from src.Maps.ixp_perc_map import IxpMapPlotting

import pickle

filename = 'D:/countrywise_as_peerllist'
infile = open(filename, 'rb')
dict_ASN_country = pickle.load(infile)
infile.close()

caidaIxp_obj = CaidaIxp('D:/AS_objects/ix-asns_202004.jsonl', 'D:/AS_objects/ixs_202004.jsonl')
ix_to_ASN, ixs_caida = caidaIxp_obj.get_data()
# print(ixs_caida)

map_obj = IxpMapPlotting(ixs_caida)
caidaixp_dict = map_obj.ixpdict()

caidaixpcleaner_obj = caidaIxpCleaner(ix_to_ASN, ixs_caida)
asn_to_ix_grouped = caidaixpcleaner_obj.group_ix_to_asn()
AS_2_IXs_caidaid = caidaixpcleaner_obj.extracter_oasn(dict_ASN_country, asn_to_ix_grouped)
# print(AS_2_IXs_caidaid)

sab = map_obj.transform_caidadict(AS_2_IXs_caidaid,caidaixp_dict)
# print(sab)

import pickle

filename = 'D:/AS_objects/ALL_ixp'

infile = open(filename, 'rb')
ALL_ixp = pickle.load(infile)
infile.close()

alles = map_obj.pdb_dict_transform(ALL_ixp)

alles_consolidated = map_obj.consolidate_pdb_caida(alles,sab)

alles_country_caidapdb  = map_obj.perc_IXP_calc(AS_2_IXs_caidaid,alles_consolidated)
# print(alles_country_caidapdb)

geo_data_cities = map_obj.geo_json_reader('D:/cities_v2.json')

map_obj.plot_IXPmap(geo_data_cities,alles_country_caidapdb)
