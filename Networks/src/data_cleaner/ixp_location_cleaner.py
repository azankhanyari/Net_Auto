from typing import Dict, List
from collections import defaultdict
import re
import pandas as pd
from src.data_fetcher.ixp_location_pdb import IxpPeeringdb
import folium
import ast
pd.options.mode.chained_assignment = None  # default='warn'


class IxpLocationCleaner:
    def __init__(self, ALL_ixp: Dict):
        self.ALL_ixp = ALL_ixp

    def extract_ixp_loc(self):

        ALL_ixp_loc_withcountry = defaultdict(list)

        for country, oasndict in self.ALL_ixp.items():
            X_ixp_loc = defaultdict(list)

            for oasn, ixdict in oasndict.items():
                inner = {}
                for ix in ixdict:
                    inner = {}
                    inner.update({'city': ix['city'], 'country': ix['country']})

                    X_ixp_loc[oasn].append(inner)

            ALL_ixp_loc_withcountry[country] = X_ixp_loc

        return ALL_ixp_loc_withcountry

    def clean_city_str(self, ALL_ixp_loc_withcountry: Dict):

        for country, val in ALL_ixp_loc_withcountry.items():
            for oasn, cclist in val.items():
                for ccpair in cclist:
                    if re.search('/', ccpair['city']):
                        ccpair['city'] = ccpair['city'].split('/', 1)[0]
                    if re.search(',', ccpair['city']):
                        ccpair['city'] = ccpair['city'].split(',', 1)[0]
                    if re.search('Los Ang', ccpair['city']):
                        ccpair['city'] = 'Los Angeles'
                    if re.search('Banga', ccpair['city']):
                        ccpair['city'] = 'Bengaluru'
                    if re.search('Frankfurt', ccpair['city']):
                        ccpair['city'] = 'Frankfurt (Oder)'
                    if re.search('Sesto', ccpair['city']):
                        ccpair['city'] = 'Florence'
                    if re.search('New York', ccpair['city']):
                        ccpair['city'] = 'New York City'
                    if re.search('Feld', ccpair['city']):
                        ccpair['city'] = 'Feldkirch'
                    if re.search('Brussels', ccpair['city']):
                        ccpair['city'] = 'Brussels'
                    if re.search('Fujairah', ccpair['city']):
                        ccpair['city'] = 'Dibba Al-Fujairah'
                    if re.search('Junin', ccpair['city']):
                        ccpair['city'] = 'Junín'

        return ALL_ixp_loc_withcountry

    def unique_pair_citycountry(self, ALL_ixp_loc_withcountry: Dict):
        city_country_dict = []
        for country, val in ALL_ixp_loc_withcountry.items():
            for oasn, countrycitylist in val.items():
                for countrycity in countrycitylist:
                    city_country_dict.append(countrycity)

        city_country_unique = [ast.literal_eval(el1) for el1 in set([str(el2) for el2 in city_country_dict])]

        return city_country_unique

    def geojson_mapper(self, geo_data_cities: Dict, city_country_unique: Dict):
        geo_Coords_city = defaultdict(list)

        for record in geo_data_cities:
            for rec2 in city_country_unique:
                if (re.search(rec2['city'], record['name']) and rec2['country'] == record['country']):
                    geo_Coords_city[rec2['city'], rec2['country']] = [record['lat'], record['lng']]

        for k, v in geo_Coords_city.items():
            inner = []
            for eachv in v:
                inner.append(float(eachv))
                geo_Coords_city[k] = inner

        return geo_Coords_city

    def mapping(self, ALL_ixp_loc_withcountry,geo_Coords_city):

        for country, dicts in ALL_ixp_loc_withcountry.items():
            if len(dicts) != 0:
                globals()['df_%s' % country] = ((pd.concat({k: pd.DataFrame(v) for k, v in dicts.items()},
                                                           axis=0).reset_index(level=1, drop=True)).reset_index())
                globals()['df_%s' % country].columns = ['AS', 'City', 'Country']

        #create tuple ley col to match with city_coords dict

        for country, dicts in ALL_ixp_loc_withcountry.items():
            if len(dicts) != 0:
                for ind in globals()['df_%s' % country].index:
                    globals()['df_%s' % country]['tup_key'] = pd.Series(list(zip(globals()['df_%s' % country].City, globals()['df_%s' % country].Country)))

        #attach coords to all df as new column

        for country, dicts in ALL_ixp_loc_withcountry.items():
            if len(dicts) != 0:
                for ind in globals()['df_%s' % country].index:
                    globals()['df_%s' % country]['coordinates'] = globals()['df_%s' % country]['tup_key'].map(geo_Coords_city)


        #remove rows with empty lat/long due to inconsistency in naming

        for country, dicts in ALL_ixp_loc_withcountry.items():
            if len(dicts) != 0:
                for ind in globals()['df_%s' % country].index:
                    globals()['df_%s' % country] = globals()['df_%s' % country][
                        globals()['df_%s' % country]['coordinates'].map(lambda d: len(d)) > 0]

        #add new column for proportion of country in df

        # for country, dicts in ALL_ixp_loc_withcountry.items():
        #     if len(dicts) != 0:
        #         for ind in globals()['df_%s' % country].index:
        #             percs = globals()['df_%s' % country]['Country'].value_counts(normalize=True) * 100
        #
        #             globals()['df_%s' % country]['percs'] = globals()['df_%s' % country]['Country'].map(percs)

        for country, dicts in ALL_ixp_loc_withcountry.items():
            if len(dicts) != 0:
                for ind in globals()['df_%s' % country].index:
                    map = folium.Map(location=[100, 0], zoom_start=2.5)

                    globals()['df_%s' % country].apply(
                        lambda row: folium.Marker(location=[(row['coordinates'][0]), (row['coordinates'][1])],popup=row['AS']).add_to(map), axis=1)
                    map.save('D:/automate_Networks/maps_IXlocs/IXP_Map_%s.html' % country)

        #drop duplicates rows whr country col same bfr plotting circle marker due to overcrowdedness

        # for country, dicts in ALL_ixp_loc_withcountry.items():
        #     if len(dicts) != 0:
        #         for ind in globals()['df_%s' % country].index:
        #             globals()['df_%s' % country] = globals()['df_%s' % country].drop_duplicates(subset=['Country', 'City'], keep='last')


        # for country, dicts in ALL_ixp_loc_withcountry.items():
        #     if len(dicts) != 0:
        #         for ind in globals()['df_%s' % country].index:
        #             map = folium.Map(location=[100, 0], zoom_start=2)
        #
        #             globals()['df_%s' % country].apply(lambda row: folium.Circle(location=[(row['coordinates'][0]), (row['coordinates'][1])],popup=row['AS'], radius=row['percs'] * 10000, color='crimson',fill=True, fill_color='crimson').add_to(map), axis=1)
        #             map.save('D:/pycharm_IXPperc/IXP_Map_%s.html' % country)


if __name__ == '__main__':
    import pickle

    filename = 'D:/AS_objects/ALL_ixp'

    infile = open(filename, 'rb')
    ALL_ixp = pickle.load(infile)
    infile.close()

    ixppdb_obj = IxpPeeringdb('https://www.peeringdb.com/api/ix?id=%s')
    # ALL_ixp = ixppdb_obj.get_ixp_data(flat_Origin_IXs)
    geo_data_cities = ixppdb_obj.geo_json_reader('D:/cities.json')

    ixploc_cleaner_obj = IxpLocationCleaner(ALL_ixp)
    ALL_ixp_loc_withcountry = ixploc_cleaner_obj.extract_ixp_loc()
    ALL_ixp_loc_withcountry = ixploc_cleaner_obj.clean_city_str(ALL_ixp_loc_withcountry)

    city_country_unique = ixploc_cleaner_obj.unique_pair_citycountry(ALL_ixp_loc_withcountry)
    geo_Coords_city = ixploc_cleaner_obj.geojson_mapper(geo_data_cities, city_country_unique)

    ixploc_cleaner_obj.mapping(ALL_ixp_loc_withcountry,geo_Coords_city)
