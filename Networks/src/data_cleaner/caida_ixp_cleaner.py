from collections import defaultdict
from typing import List,Dict


from src.data_fetcher.caida_ixp_data import CaidaIxp

class caidaIxpCleaner:

    def __init__(self,ix_to_ASN:List,ixs_caida:List):
        self.ix_to_ASN = ix_to_ASN
        self.ixs_caida = ixs_caida

    def group_ix_to_asn(self):
        asn_to_ix_grouped = defaultdict(list)

        for record in self.ix_to_ASN:
            asn_to_ix_grouped[record['asn']].append(record['ix_id'])

        return  asn_to_ix_grouped

    def extracter_oasn(self, dict_ASN_country:Dict, asn_to_ix_grouped:Dict):
        AS_2_IXs_caidaid = {}
        inner_dict = {}

        for country, OASlist in dict_ASN_country.items():
            AS_2_IXs_caidaid.update({country: []})
            inner_dict = {}

            for oasn in OASlist:

                inner_dict.update({oasn: []})

                for asn, ix_id in asn_to_ix_grouped.items():

                    if str(asn) == oasn:
                        inner_dict[oasn].extend(ix_id)

            AS_2_IXs_caidaid[country] = inner_dict

        return  AS_2_IXs_caidaid

    def extracter_peer(self,dict_ASN_country:Dict,asn_to_ix_grouped:Dict):

        country_mains = {}
        for country in dict_ASN_country:
            mains = {}
            country_mains.update({country: []})

            ASN_X_peers = [dict_ASN_country[country]]

            inner = defaultdict(list)

            for rec in ASN_X_peers:
                for asn, peers in rec.items():
                    inner = defaultdict(list)
                    mains.update({asn: []})

                    if len(peers) != 0:
                        peerlist = peers
                        for peer in peerlist:
                            for asn2, ix_ids in asn_to_ix_grouped.items():
                                if str(asn2) == peer:
                                    inner[peer].extend(ix_ids)
                                    break
                    mains[asn] = inner

            country_mains[country] = mains

        return  country_mains


    def common_ix(self,country:str,country_mains:Dict,AS_2_IXs_caidaid:Dict):

        set_common = {}
        peer = country_mains[country]
        origin = AS_2_IXs_caidaid[country]

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

    def iterate_common_ix(self,country_mains:Dict,AS_2_IXs_caidaid:Dict):
        common_all_facs = {}

        for country in AS_2_IXs_caidaid:
            z = self.common_ix(country,country_mains,AS_2_IXs_caidaid)
            common_all_facs.update({country: z})

        return common_all_facs

    def find_nocommon(self,country:str, common_all_facs:Dict):

        common_IX = common_all_facs[country]
        dict1 = {}

        for oasn, peerdict in common_IX.items():

            dict1.update({oasn: []})

            if len(peerdict) != 0:
                for peer, ixid in peerdict.items():
                    if len(ixid) == 0:
                        dict1[oasn].append(peer)
        return dict1

    def iterate_find_nocommon(self,dict_ASN_country:Dict,common_all_facs:Dict):
        no_common_ALL = {}
        for country in dict_ASN_country:
            z = self.find_nocommon(country, common_all_facs)
            no_common_ALL.update({country: z})

        return  no_common_ALL













if __name__ == '__main__':
    import pickle

    filename = 'D:/countrywise_as_peerllist'   # from caida_cleaner.py
    infile = open(filename, 'rb')
    dict_ASN_country = pickle.load(infile)
    infile.close()

    caidaIxp_obj = CaidaIxp('D:/AS_objects/ix-asns_202004.jsonl', 'D:/AS_objects/ixs_202004.jsonl')
    ix_to_ASN, ixs_caida = caidaIxp_obj.get_data()

    caidaixpcleaner_obj = caidaIxpCleaner(ix_to_ASN, ixs_caida)
    asn_to_ix_grouped = caidaixpcleaner_obj.group_ix_to_asn()

    AS_2_IXs_caidaid = caidaixpcleaner_obj.extracter_oasn(dict_ASN_country, asn_to_ix_grouped)
    print(AS_2_IXs_caidaid)

    country_mains = caidaixpcleaner_obj.extracter_peer(dict_ASN_country, asn_to_ix_grouped)

    common_all_facs = caidaixpcleaner_obj.iterate_common_ix(country_mains,AS_2_IXs_caidaid)

    no_common_ALL = caidaixpcleaner_obj.iterate_find_nocommon(dict_ASN_country,common_all_facs)

    import pickle

    filename = 'D:/no_common_ALL'   #final object
    outfile = open(filename, 'wb')
    pickle.dump(no_common_ALL, outfile)
    outfile.close()
















