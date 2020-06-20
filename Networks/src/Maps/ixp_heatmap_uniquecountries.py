
from collections import defaultdict
import pickle
import folium
import pandas as pd


class IxpHeatmapUniqueCountry:
    def __init__(self,IXs_locs_pdb):
        self.IXs_locs_pdb = IXs_locs_pdb


    def find_out_of_home_country(self):
        # keep only entries where country is out of home country

        noncountry = {country: {tupky: asn for tupky, asn in dct.items() if tupky[3] != country} for country, dct in self.IXs_locs_pdb.items()}

        return noncountry

    def unique_country_finder(self,noncountry):
        # make a dict key on unique countries that asns peer in
        outer = defaultdict(list)

        for c, v in noncountry.items():
            uniq_dict = defaultdict(set)
            for tp, asn in v.items():
                for as1 in asn:
                    uniq_dict[tp[3]].add(as1)
            outer[c] = uniq_dict

        return outer

    def perc_uc(self,outer,heatmap):
        # perc = no of asn in country/ no of asn peering outside home country

        mains = defaultdict(list)
        for c, v in outer.items():
            counter_dct = defaultdict(list)
            for c2, asns in v.items():
                counter_dct[c2] = len(asns) / len(heatmap[c]) * 100
            mains[c] = counter_dct

        return mains

    def hm_plotter(self,mains,path_countryGeojson):
        # make df

        for country, oasndict in mains.items():
            if len(oasndict) != 0:
                globals()['df_%s' % country] = pd.Series(oasndict).reset_index()

                globals()['df_%s' % country].columns = ['country', 'percentage']

        # make HEATMAP
        for country, dct in mains.items():
            if len(dct) != 0:
                map = folium.Map(location=[100, 0], zoom_start=1.5)

                df = globals()['df_%s' % country]

                map.choropleth(geo_data=path_countryGeojson, data=df,
                               columns=['country', 'percentage'],
                               key_on='properties.wb_a2',
                               fill_color='YlGnBu', fill_opacity=0.7, line_opacity=0.2)

                map.save('D:/automate_Networks/uc_heatmaps/heatmap_%s.html' % country)


if __name__ == '__main__':
    with open('D:/automate_Networks/IXs_locs_pdb', 'rb') as infile:
        IXs_locs_pdb = pickle.load(infile)

    with open('D:/automate_Networks/heatmap', 'rb') as infile:
        heatmap = pickle.load(infile)

    hm_uc_obj = IxpHeatmapUniqueCountry(IXs_locs_pdb)
    noncountry= hm_uc_obj.find_out_of_home_country()
    outer = hm_uc_obj.unique_country_finder(noncountry)
    mains = hm_uc_obj.perc_uc(outer,heatmap)
    hm_uc_obj.hm_plotter(mains,'D:/country_geo.geojson')




