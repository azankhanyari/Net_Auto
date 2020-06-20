from collections import defaultdict
from typing import List

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

from src.data_fetcher.caida_peer import CaidaPeer
from src.data_fetcher.caida_relation import AsRelationship


class CaidaTransform:
    def __init__(self, rawdata, caida_peer: pd.DataFrame):
        self.caida_df = pd.DataFrame(rawdata)
        self.caida_peer = caida_peer

    def transform(self):
        df_caida_country = self.group_by_country()
        df_filter = self.filter_column(df_caida_country)
        df_filter = self.numeric_rank(df_filter)
        df_sorted = self.sort(df_filter)
        dict_asn_country = self.create_dict_mapper(df_sorted)
        all_peer_per_country = self.country_asn_peer_mapper(dict_asn_country)
        # print("All peer country")
        # print(all_peer_per_country)
        return all_peer_per_country

    def group_by_country(self):

        df_caidacountry = []

        for country_name, df_bycountry in self.caida_df.groupby('country_name'):
            df_caidacountry.append(df_bycountry)
        return df_caidacountry

    def filter_column(self, df_caidacountry: List[pd.DataFrame], column: List = ['id', 'rank', 'country']):
        return [df[column] for df in df_caidacountry]

    def numeric_rank(self, df_filter: List[pd.DataFrame]):
        for df in df_filter:
            df['rank'] = pd.to_numeric(df['rank'])
        return df_filter

    def sort(self, df_filter: List[pd.DataFrame], top: int = 100):

        return [df.sort_values('rank', ascending=True).head(100) for df in df_filter]

    def create_dict_mapper(self, df_caida_sort: List[pd.DataFrame]):

        return {df['country'].iloc[0]: list(df['id']) for df in df_caida_sort}

    def get_peer(self, AS: str):
        return self.caida_peer[(self.caida_peer['AS'] == AS) & (self.caida_peer['is_peer'] == 1)]['peer'].to_list()

    def country_asn_peer_mapper(self, dict_asn_country):
        all_peer_per_country = defaultdict(list)

        for country, oasn in dict_asn_country.items():
            all_peers_dict = {}

            for asn in oasn:
                all_peer = self.get_peer(str(asn))
                all_peers_dict.update({asn: all_peer})
            all_peer_per_country[country] = all_peers_dict
        return all_peer_per_country


if __name__ == '__main__':
    as_object = AsRelationship('http://as-rank-test.caida.org/api/v1/asns?populate=1&page=%s&sort=customer_cone_addresses', 1698)
    raw_data = as_object.get_rank_data()['data']
    # filename = 'D:/caida_raw_data'
    # outfile = open(filename, 'wb')
    # pickle.dump(raw_data, outfile)

    # outfile.close()
    caida_peer_obj = CaidaPeer('C:/Users/Azaan/Downloads/Compressed/20200401.as-rel.txt/20200401.as-rel.txt')
    caida_peer_df = caida_peer_obj.get_data()
    caida_obj = CaidaTransform(raw_data, caida_peer_df)
    print("Transforming")
    final = caida_obj.transform()    #dict_ASN_country
    # filename = 'D:/final'
    # outfile = open(filename, 'wb')
    # pickle.dump(final, outfile)
    #
    # outfile.close()

    # print(final)
