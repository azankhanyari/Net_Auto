import requests
import ssl
import urllib3
from typing import Dict
import json
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.mode.chained_assignment = None  # default='warn'

class IxpPeeringdb:
    def __init__(self, ixp_url: str):
        self.ixp_url = ixp_url

    def get_ixp_data(self,flat_Origin_IXs:Dict):
        ALL_ixp = {}

        for country, oasndict in flat_Origin_IXs.items():
            X_ixp = {}
            for oasn, ix_ids in oasndict.items():
                for ix_id in ix_ids:
                    print(country, oasn, ix_id)
                    resp = requests.get(self.ixp_url % ix_id, verify=False)
                    data = resp.json()
                    data = data['data']
                    if oasn not in X_ixp:
                        X_ixp.update({oasn: data})
                    else:
                        X_ixp[oasn].extend(data)
            ALL_ixp[country] = X_ixp

        return ALL_ixp

    def geo_json_reader(self,path:str):

        with open(path, encoding="utf-8") as f:
            geo_data_cities = json.load(f)

        return  geo_data_cities




if __name__ == '__main__':
    import pickle

    filename = 'D:/flat_Origin_IXs'

    infile = open(filename, 'rb')
    flat_Origin_IXs = pickle.load(infile)
    infile.close()

    filename = 'D:/AS_objects/ALL_ixp'

    infile = open(filename, 'rb')
    ALL_ixp = pickle.load(infile)
    infile.close()

    ixppdb_obj = IxpPeeringdb('https://www.peeringdb.com/api/ix?id=%s')
    # ALL_ixp = ixppdb_obj.get_ixp_data(flat_Origin_IXs)







