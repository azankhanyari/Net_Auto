from typing import Dict,List


from src.data_fetcher.peeringdb_data import PeeringdbPeer


class PeeringdbClean:
    def __init__(self, Origins_ALLcountry: Dict, mega_list:Dict):
        self.Origins_ALLcountry = Origins_ALLcountry
        self.mega_list = mega_list


    def get_origin_netfacs_IX(self, country: str):
        Origin_X_netfacs = {}
        Origin_X_IXs = {}

        a = self.Origins_ALLcountry[country]
        for rec1 in a:
            b = a[rec1]
            Origin_X_netfacs = self.get_inner_id(rec1, b, Origin_X_netfacs, 'netfac_set', 'fac_id')
            Origin_X_IXs = self.get_inner_id(rec1, b, Origin_X_IXs, 'netixlan_set', 'ix_id')

        return Origin_X_netfacs, Origin_X_IXs



    def get_inner_id(self, rec1: str, b: List, dict_storage: Dict, key_name: str, child_key: str):
        if len(b) == 0:
            # print(rec1)
            dict_storage.update({rec1: []})
        else:
            for c in b:

                nets = c[key_name]

                if len(nets) == 0:
                    dict_storage.update({rec1: []})
                else:
                    for d in nets:
                        if rec1 not in dict_storage:
                            dict_storage.update({rec1: [d[child_key]]})
                        else:
                            dict_storage[rec1].extend([d[child_key]])
        return dict_storage



    def iterate_originas_netfacs_IX(self):
        flat_Origin_netfacs = {}
        flat_Origin_IXs = {}
        for country in self.Origins_ALLcountry:
            wn = self.get_origin_netfacs_IX(country)
            flat_Origin_netfacs.update({country: wn[0]})
            flat_Origin_IXs.update({country: wn[1]})

        return flat_Origin_netfacs, flat_Origin_IXs




    def get_peer_netfacs_IX(self, country:str):

        peer_X_netfacs = {}
        peer_X_IXs = {}


        peer_X_netfacs = self.get_inner_peer_id(country, peer_X_netfacs, 'netfac_set', 'fac_id')
        peer_X_IXs = self.get_inner_peer_id(country, peer_X_IXs, 'netixlan_set', 'ix_id')

        return peer_X_netfacs, peer_X_IXs

    def get_inner_peer_id(self, country: str, dict_storage: Dict, key_name: str, child_key: str):

        a = self.mega_list[country]

        for oasn, val1 in a.items():
            i = 0
            k = 0


            if len(val1) == 0:
                dict_storage.update({oasn: []})

            for listitem in val1:

                for innerdict in listitem:

                    netfacs = innerdict[key_name]
                    p = str(innerdict['asn'])
                    if len(netfacs) == 0:
                        if k == 0:
                            dict_storage.update({oasn: {p: []}})
                            k = k + 1
                            i = i + 1
                        if p not in dict_storage[oasn]:
                            dict_storage[oasn].update({p: []})

                    for fac in netfacs:

                        peerasn = str(innerdict['asn'])
                        if i == 0:
                            dict_storage.update({oasn: {peerasn: []}})
                            i = i + 1
                            k = k + 1

                        if peerasn not in dict_storage[oasn]:

                            dict_storage[oasn].update({peerasn: [fac[child_key]]})
                        else:

                            dict_storage[oasn][peerasn].extend([fac[child_key]])

        return dict_storage

    def iterate_peeras_netfacs_IX(self):
        flat_peer_netfacs = {}
        flat_peer_IXs = {}

        for country in self.mega_list:
            pn = self.get_peer_netfacs_IX(country)
            flat_peer_netfacs.update({country: pn[0]})
            flat_peer_IXs.update({country: pn[1]})

        return flat_peer_netfacs, flat_peer_IXs

############################################# test ########################

    def common(self, country:str,flat_peer_netfacs,flat_Origin_netfacs,flat_peer_IXs,flat_Origin_IXs):


        peer = flat_peer_netfacs[country]
        origin = flat_Origin_netfacs[country]
        peer2 = flat_peer_IXs[country]
        origin2 = flat_Origin_IXs[country]

        set_common_facs = self.common_inner(peer, origin)
        set_common_IXs  = self.common_inner(peer2, origin2)

        return set_common_facs, set_common_IXs

    def common_inner(self, peer:Dict, origin:Dict ):

        set_common = {}
        for oasn1, fac in origin.items():

            i = 0

            for oasn2, peerdict in peer.items():
                if oasn1 == oasn2:

                    if len(peerdict) == 0:
                        set_common.update({oasn1: set()})
                    else:
                        for pasn, pfacs in peerdict.items():
                            if i == 0:
                                set_common.update({oasn1: {pasn: []}})
                                i = i + 1

                            if pasn not in set_common[oasn1]:
                                set_common[oasn1].update({pasn: []})

                            common = set(fac).intersection(pfacs)

                            set_common[oasn1][pasn].extend(common)

        return set_common

    def iterate_common(self, flat_peer_netfacs,flat_Origin_netfacs,flat_peer_IXs,flat_Origin_IXs):

        common_all_facs = {}
        common_all_IXs = {}

        for country in self.mega_list:
            cn = self.common(country,flat_peer_netfacs,flat_Origin_netfacs,flat_peer_IXs,flat_Origin_IXs)
            common_all_facs.update({country: cn[0]})
            common_all_IXs.update({country: cn[1]})

        return common_all_facs, common_all_IXs

    ############## final test ###############

    def intersection_all(self, country:str,common_all_IXs,common_all_facs):

        ix = common_all_IXs[country]
        netfac = common_all_facs[country]

        big_one = {}

        for oasn1, ixdict in ix.items():
            nocommon = {}
            if len(ixdict) != 0:

                for peerasn1, commonix in ixdict.items():
                    if len(commonix) == 0 and len(netfac[oasn1][peerasn1]) == 0:
                        nocommon.update({peerasn1: []})
                if len(nocommon) != 0:
                    big_one.update({oasn1: nocommon})

        return big_one

    def iterate_intersection(self,common_all_IXs,common_all_facs):
        No_intersev = {}

        for country in self.mega_list:
            z = self.intersection_all(country,common_all_IXs,common_all_facs)
            No_intersev.update({country: z})

        return No_intersev





if __name__ == '__main__':
    import pickle

    peeringdb_obj = PeeringdbPeer('https://www.peeringdb.com/api/net?asn=%s&depth=3')

    filename = 'D:/countrywise_as_peerllist'    #this file produced by caida_cleaner.py
    infile = open(filename, 'rb')
    dict_ASN_country = pickle.load(infile)
    infile.close()


    peer_dict = peeringdb_obj.get_peer_data(dict_ASN_country)  ## download data from pdb long time   D:\AS_objects\super_dict
    # print(peer_dict)
    origin_dict = peeringdb_obj.iterate_originas_data(dict_ASN_country) ### D:\AS_objects\Origins_ALLcountry
    # print(origin_dict)

    peeringdbclean_obj = PeeringdbClean(origin_dict,peer_dict)

    flat_Origin_netfacs, flat_Origin_IXs = peeringdbclean_obj.iterate_originas_netfacs_IX()
    # print(flat_Origin_netfacs)
    # print(flat_Origin_IXs)

    flat_peer_netfacs, flat_peer_IXs = peeringdbclean_obj.iterate_peeras_netfacs_IX()
    # print(flat_peer_netfacs)
    # print(flat_peer_IXs)

    common_all_facs, common_all_IXs = peeringdbclean_obj.iterate_common(flat_peer_netfacs,flat_Origin_netfacs,flat_peer_IXs,flat_Origin_IXs)
    # print(common_all_facs)
    # print(common_all_IXs)

    No_intersection = peeringdbclean_obj.iterate_intersection(common_all_IXs,common_all_facs)
    # print(No_intersection)

    filename = 'D:/automate_Networks/flat_Origin_IXs'  # needed for file ixp_
    outfile = open(filename, 'wb')
    pickle.dump(flat_Origin_IXs, outfile)
    outfile.close()

    filename = 'D:/automate_Networks/No_intersection'  #final needed object
    outfile = open(filename, 'wb')
    pickle.dump(No_intersection, outfile)
    outfile.close()

