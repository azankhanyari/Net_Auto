from collections import defaultdict
import pickle
import json
import re
import pandas as pd
import folium


class AllHeatmap:
    def __init__(self,IXs_locs_pdb,ALL_ixp):
        self.IXs_locs_pdb = IXs_locs_pdb
        self.ALL_ixp = ALL_ixp

    def Isp_counter(self):
        # find no of ISP peering outsite a particular country
        heatmap = defaultdict(list)
        li = set()

        for country, val in self.IXs_locs_pdb.items():
            li = set()
            if country == 'EU':
                for tupkey, asns in val.items():
                    if tupkey[3] not in ['FR', 'IT', 'DE', 'IE', 'ES', 'SE', 'FI', 'NL', 'DK', 'LT', 'GR', 'PL', 'RO',
                                         'LU', 'SK', 'HR', 'BG', 'LV', 'AT', 'EE', 'HU', 'BE', 'CZ', 'PT']:
                        for as1 in asns:
                            li.add(as1)



            else:
                for tupkey, asns in val.items():
                    if tupkey[3] != country:
                        for as1 in asns:
                            li.add(as1)
            heatmap[country] = li
        return heatmap

    def Isp_perc(self,heatmap):
        ####count tpercentage of ISP peering outside of home country

        heatmap2 = defaultdict(list)
        for country, st in heatmap.items():

            try:
                heatmap2[country] = len(st) / len(self.ALL_ixp[country]) * 100  # denominator = no of country-specific ISP for whcih we have IXP info
            except ZeroDivisionError:
                pass
        return heatmap2

    def geo_json_reader(self, path: str):

        with open(path, encoding="utf-8") as f:
            geo_country = json.load(f)

        geo_country_dict = defaultdict(list)

        for rec in geo_country:
            geo_country_dict[rec['country_code']] = rec['latlng']

        return geo_country_dict

    def coordinate_mapper(self, geo_country_dict, heatmap2):
        ### need to extract coordinates for all city,country pairs and match with geo_json

        geo_Coords_city = defaultdict(list)

        for country in heatmap2:
            if country in geo_country_dict:
                geo_Coords_city[country] = geo_country_dict[country]

        return geo_Coords_city

    def heatmapping(self,heatmap2,geo_Coords_city,path_countryGeojson):
        # make df

        heatmap_df = pd.DataFrame.from_dict(heatmap2, orient='index').reset_index()
        heatmap_df.columns = ['country', 'perc']

        ############# attach coords to all df as new column

        heatmap_df['coordinates'] = heatmap_df['country'].map(geo_Coords_city)

        map = folium.Map(location=[100, 0], zoom_start=1.5)

        map.choropleth(geo_data=path_countryGeojson, data=heatmap_df,
                   columns=['country', 'perc'],
                   key_on='properties.wb_a2',
                   fill_color='YlGnBu', fill_opacity=0.7, line_opacity=0.2
                   )


        map.save('D:/automate_Networks/heatmapALL/IXP_Heatmap.html')


if __name__ == '__main__':

    with open('D:/automate_Networks/IXs_locs_pdb', 'rb') as infile:
        IXs_locs_pdb = pickle.load(infile)

    with open('D:/automate_Networks/ALL_ixp', 'rb') as infile:
        ALL_ixp = pickle.load(infile)

    hm_obj = AllHeatmap(IXs_locs_pdb,ALL_ixp)
    heatmap = hm_obj.Isp_counter()
    heatmap2 = hm_obj.Isp_perc(heatmap)
    geo_country_dict = hm_obj.geo_json_reader('D:/countries.json')
    geo_Coords_city = hm_obj.coordinate_mapper(geo_country_dict,heatmap2)
    hm_obj.heatmapping(heatmap2,geo_Coords_city,'D:/country_geo.geojson')
