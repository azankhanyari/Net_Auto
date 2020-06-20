from typing import Dict
from urllib.error import HTTPError

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class PeeringdbPeer:
    def __init__(self, peeringb_url: str):
        self.peeringdb_url = peeringb_url

    def get_peer_data(self, dict_ASN_country: Dict):

        mega_list = {}

        for country, asndicts in dict_ASN_country.items():
            ALL_X = {}
            for oasn, peers in asndicts.items():
                if len(peers) == 0:
                    ALL_X.update({oasn: []})
                else:
                    for peer in peers:
                        print(peer)
                        try:
                            resp = requests.get(self.peeringdb_url % peer, verify=False)
                            data = resp.json()
                            data = data['data']
                            if oasn not in ALL_X:
                                ALL_X.update({oasn: [data]})
                            else:
                                ALL_X[oasn].extend([data])
                        except HTTPError as err:
                            if err.code == 404:
                                ALL_X.update({oasn: []})
                            else:
                                raise
            mega_list.update({country: ALL_X})
        return mega_list

    def get_originasn_data(self, country: str, dict_ASN_country: Dict):
        Origins_X = {}
        for ix in dict_ASN_country[country]:
            print(ix)

            try:
                resp = requests.get(self.peeringdb_url % ix, verify=False)
                data = resp.json()
                data = data['data']
                if ix not in Origins_X:
                    Origins_X.update({ix: data})
                else:
                    Origins_X[ix].extend(data)
            except:
                Origins_X.update({ix: []})
        return Origins_X

    def iterate_originas_data(self, dict_ASN_country: Dict):
        Origins_ALLcountry = {}
        for country in dict_ASN_country:
            z = self.get_originasn_data(country, dict_ASN_country)
            Origins_ALLcountry.update({country: z})
        return Origins_ALLcountry


if __name__ == '__main__':
    import pickle

    peeringdb_obj = PeeringdbPeer('https://www.peeringdb.com/api/net?asn=%s&depth=3')

    filename = 'D:/countrywise_as_peerllist'
    infile = open(filename, 'rb')
    dict_ASN_country = pickle.load(infile)
    infile.close()
    dict_ASN_country = {'AD': dict_ASN_country['AD']}
    print(peeringdb_obj.get_peer_data(dict_ASN_country))
    print(peeringdb_obj.iterate_originas_data(dict_ASN_country))
