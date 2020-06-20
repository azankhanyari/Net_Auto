

import pandas as pd
from collections import defaultdict
import json
import folium
import re
from folium.plugins import MarkerCluster
from src.data_fetcher.caida_ixp_data import CaidaIxp
from src.data_cleaner.caida_ixp_cleaner import caidaIxpCleaner
from src.data_cleaner.ixp_location_cleaner import IxpLocationCleaner





class IxpMapPlotting:
     def __init__(self,ixs_caida):
         self.ixs_caida = ixs_caida

     def ixpdict(self):
         caidaixp_dict = defaultdict(list)
         for dct in self.ixs_caida:
             caidaixp_dict[dct['ix_id']] = (dct.get('name'), dct.get('city'), dct.get('country'), dct.get('pdb_id'))

         # for item in self.ixs_caida:
         #     if 'city' in item and 'country' in item and 'pdb_id' in item:
         #         caidaixp_dict[item['ix_id']] = (item['name'], item['city'], item['country'], item['pdb_id'])
         #     elif 'country' in item and 'pdb_id' in item:
         #         caidaixp_dict[item['ix_id']] = (item['name'], '', item['country'], item['pdb_id'])
         #     elif 'country' in item:
         #         caidaixp_dict[item['ix_id']] = (item['name'], '', item['country'], '')
         #     elif 'city' in item and 'pdb_id' in item:
         #         caidaixp_dict[item['ix_id']] = (item['name'], item['city'], '', item['pdb_id'])
         #
         #
         #     else:
         #         caidaixp_dict[item['ix_id']] = (item['name'], '', '', '')
         return caidaixp_dict

     def transform_caidadict(self,AS_2_IXs_caidaid,caidaixp_dict):
         # extract and flatten needed IXP + asns that peer at them
         sab = defaultdict(list)

         for country, oasndict in AS_2_IXs_caidaid.items():
             IXP_dict_caida = defaultdict(list)
             for oasn, ixplist in oasndict.items():
                 if len(ixplist) != 0:
                     for ix in ixplist:
                         if ix in caidaixp_dict:
                             IXP_dict_caida[ix, str(caidaixp_dict[ix][0]), str(caidaixp_dict[ix][1]), str(caidaixp_dict[ix][2]), str(caidaixp_dict[ix][3])].append(oasn)

                 sab[country] = IXP_dict_caida


         return sab

     def pdb_dict_transform(self,ALL_ixp):
         # extract and flatten needed IXP + asns that peer at them
         alles = defaultdict(list)
         for country, oasndict in ALL_ixp.items():
             IXP_dict_v3 = defaultdict(list)

             for oasn, ixplist in oasndict.items():
                 for li2 in ixplist:
                     IXP_dict_v3[li2['id'], li2['name'], li2['city'], li2['country']].append(oasn)

             alles[country] = IXP_dict_v3
         return alles


     def consolidate_pdb_caida(self,alles,sab):
         ## compare ixp_pdb with ixp_caida on pdb_keys and add asns if found

         for k, v in alles.items():
             if len(v) != 0:
                 for c, a in sab.items():
                     if len(a) != 0:
                         for k2, v2 in v.items():
                             for c2, a2 in a.items():
                                 if (k == c) and str(k2[0]) == c2[4]:
                                     v2.extend(a2)

         ##set to remove duplicate asn

         for k, v in alles.items():
             if len(v) != 0:
                 for k2, v2 in v.items():
                     v[k2] = list(set(v2))

         return alles


     def perc_IXP_calc(self,AS_2_IXs_caidaid,alles_consolidated):
        # need to get count of oasns for which we have ixp location info
        outerdict = defaultdict(list)
        for country, oasndict in AS_2_IXs_caidaid.items():
            newdict = defaultdict(list)
            for oasn, ixplist in oasndict.items():
                if len(ixplist) != 0:
                    newdict[oasn] = ixplist
            outerdict[country] = newdict

        #### count no of asn perc

        alles_country_caidapdb = defaultdict(list)

        for country, oasndict in alles_consolidated.items():

            alles_caida_pdb = defaultdict(list)
            for ixpdata, oasnlist in oasndict.items():
                alles_caida_pdb[ixpdata] = len(set(oasnlist)) / len(outerdict[country]) * 100  # denominator = no of country-specific ISP for whcih we have IXP info
            alles_country_caidapdb[country] = alles_caida_pdb

        return alles_country_caidapdb

     def geo_json_reader(self, path: str):

         with open(path, encoding="utf-8") as f:
             geo_data_cities = json.load(f)

         return geo_data_cities


     def plot_IXPmap(self,geo_data_cities,alles_country_caidapdb):
         # make df

         for country, oasndict in alles_country_caidapdb.items():
             if len(oasndict) != 0:
                 globals()['df_%s' % country] = pd.Series(oasndict).reset_index()

                 globals()['df_%s' % country].columns = ['ix_id', 'ix_name', 'city', 'country', 'perc']

         ### need to extract coordinates for all city,country pairs and match with geo_json

         geo_Coords_city = defaultdict(list)

         for record in geo_data_cities:
             for country, value in alles_country_caidapdb.items():
                 for k, v in value.items():
                     if (re.search(k[2], record['name']) and k[3] == record['country']):
                         geo_Coords_city[k[2], k[3]] = [record['lat'], record['lng']]

         ####################################### chnge coords to float
         for k, v in geo_Coords_city.items():
             inner = []
             for eachv in v:
                 inner.append(float(eachv))
                 geo_Coords_city[k] = inner

         #######################################create tuple key

         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:
                 for ind in globals()['df_%s' % country].index:
                     globals()['df_%s' % country]['tup_key'] = pd.Series(
                         list(zip(globals()['df_%s' % country].city, globals()['df_%s' % country].country)))

        ############# attach coords to all df as new column

         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:
                 for ind in globals()['df_%s' % country].index:
                     globals()['df_%s' % country]['coordinates'] = globals()['df_%s' % country]['tup_key'].map(
                         geo_Coords_city)

         ##get rid of df with no coordinates in column empty list
         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:
                 for ind in globals()['df_%s' % country].index:
                     globals()['df_%s' % country] = globals()['df_%s' % country][
                         globals()['df_%s' % country]['coordinates'].map(lambda d: len(d)) > 0]

         ###################################################
         # make lat,long column separately for folium
         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:
                 for ind in globals()['df_%s' % country].index:
                     df = globals()['df_%s' % country]

                     globals()['df_%s' % country] = globals()['df_%s' % country].assign(lat='', long='')

         ######################## unpack value to empty lat,long
         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:
                 for ind in globals()['df_%s' % country].index:
                     df = globals()['df_%s' % country]

                     df['lat'][ind] = df['coordinates'][ind][0]
                     df['long'][ind] = df['coordinates'][ind][1]

         ############### mapping wth clusters
         def color_producer(perc):
             if perc < 5:
                 return 'red'
             elif 5 <= perc < 10:
                 return 'orange'
             elif 10 <= perc < 20:
                 return 'lime'
             elif 20 <= perc < 30:
                 return 'blue'
             elif 30 <= perc < 40:
                 return 'gray'
             elif 40 <= perc < 50:
                 return 'sienna'
             else:
                 return 'green'

         for country, dicts in alles_country_caidapdb.items():
             if len(dicts) != 0:

                 for ind in globals()['df_%s' % country].index:
                     df = globals()['df_%s' % country]
                     map = folium.Map(location=[100, 0], zoom_start=2)
                     mc = MarkerCluster()
                     for lat, lon, name, perc in zip(df['lat'], df['long'], df['ix_name'], df['perc']):
                         mc.add_child(folium.CircleMarker(location=[lat, lon],
                                                          radius=perc,
                                                          popup=(name, round(perc, 1)),
                                                          # tooltip= perc,
                                                          fill=True,  # Set fill to True
                                                          fill_color=color_producer(perc),
                                                          color=color_producer(perc)))
                     map.add_child(mc)
                     legend_html = '''
                         <div style="position: fixed; 
                                     bottom: 50px; left: 50px; width: 200px; height: 200px; 
                                     border:2px solid grey; z-index:9999; font-size:12px;
                                     ">&nbsp;<b> Legend </b><br>
                                       &nbsp; perc < 5 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:red"></i><br>
                                       &nbsp; 5 <= perc < 10 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:orange"></i><br>
                                       &nbsp; 10 <= perc < 20 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:lime"></i><br>
                                       &nbsp; 20 <= perc < 30 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:blue"></i><br>
                                       &nbsp; 30 <= perc < 40 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:grey"></i><br>
                                       &nbsp; 40 <= perc < 50 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:sienna"></i><br>
                                       &nbsp; perc > 50 &nbsp; <i class="fa fa-circle-o fa-2x" aria-hidden="true" style="color:green"></i><br>
                         </div>
                         '''
                     map.get_root().html.add_child(folium.Element(legend_html))

                     map.save('D:/automate_Networks/ixp_perc/IXP_Map_%s.html' % country)




if __name__ == '__main__':
    from pathlib import Path
    # import sys
    #
    # sys.path.append(r'D:\PycharmProjects\Networks\src\data_cleaner')
    # sys.path.append(r'D:\PycharmProjects\Networks\src\data_fetcher')
    # sys.path.append(r'D:\PycharmProjects\Networks\src\Maps')


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






