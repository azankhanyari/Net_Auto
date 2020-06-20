import jsonlines
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'



class CaidaIxp:
    def __init__(self, path: str,path2:str):
        self.path = path
        self.path2 = path2

    def get_data(self):
        ix_to_ASN = []
        with jsonlines.open(self.path, mode='r') as reader:
            for obj in reader:
                ix_to_ASN.append(obj)

        ixs_caida = []
        with jsonlines.open(self.path2, mode='r') as reader:
            for obj in reader:
                ixs_caida.append(obj)

        return ix_to_ASN, ixs_caida


if __name__ == '__main__':
    import pickle
    caidaIxp_obj = CaidaIxp('D:/AS_objects/ix-asns_202004.jsonl','D:/AS_objects/ixs_202004.jsonl')
    ix_to_ASN, ixs_caida= caidaIxp_obj.get_data()
    print(ix_to_ASN)
    print(ixs_caida)




















    # filename = 'D:/ix_to_asn'
    # outfile = open(filename, 'wb')
    # pickle.dump(ix_to_ASN, outfile)
    # outfile.close()


# filename = 'D:/ix_to_asn'
# infile = open(filename, 'rb')
# ix_2_asn = pickle.load(infile)
# infile.close()