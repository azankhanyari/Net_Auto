import urllib3
import pickle

from src.Maps.ixp_heatmap_allcountry import AllHeatmap
from src.Maps.ixp_heatmap_uniquecountries import IxpHeatmapUniqueCountry
from src.data_cleaner.caida_cleaner import CaidaTransform
import configparser
from pathlib import Path
from src.data_cleaner.peerindb_cleaner import PeeringdbClean
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from src.data_fetcher.ixp_location_pdb import IxpPeeringdb
from src.data_cleaner.caida_ixp_cleaner import caidaIxpCleaner
from src.data_cleaner.ixp_location_cleaner import IxpLocationCleaner
from src.Maps.ixp_perc_map import IxpMapPlotting
from src.data_fetcher.peeringdb_data import PeeringdbPeer
from src.data_fetcher.caida_peer import CaidaPeer
from src.data_fetcher.caida_relation import AsRelationship
from src.data_fetcher.caida_ixp_data import CaidaIxp


class caida_peer:

    def caida_cleaner(self, file_rawdata, use_cache, asrank_url, maxpage, caidapeer_file):
        # cache_store = None
        if use_cache:
            try:
                with open(file_rawdata, 'rb') as infile:
                    raw_data = pickle.load(infile)

            except FileNotFoundError:
                raw_data = self.download(asrank_url, maxpage)
        else:
            # print('Downloading Files Now.....')
            raw_data = self.download(asrank_url, maxpage)

        final = self.caida_transform(caidapeer_file, raw_data)

        self.save_cache(final)

    def save_cache(self, final):
        filename = 'D:/automate_Networks/dict_ASN_country'
        outfile = open(filename, 'wb')
        pickle.dump(final, outfile)
        outfile.close()

    def caida_transform(self, caidapeer_file, raw_data):
        caida_peer_obj = CaidaPeer(caidapeer_file)
        caida_peer_df = caida_peer_obj.get_data()
        caida_obj = CaidaTransform(raw_data, caida_peer_df)
        print("Transforming")
        final = caida_obj.transform()  # dict_ASN_country
        return final

    def download(self, asrank_url, maxpage):
        print('No Local file found, Downloading Files now')
        as_object = AsRelationship(asrank_url, maxpage)
        raw_data = as_object.get_rank_data()['data']
        filename = 'D:/automate_Networks/caida_raw_data'
        outfile = open(filename, 'wb')
        pickle.dump(raw_data, outfile)
        return raw_data


class pdb:

    def pdb_cleaner(self, file_dictASNcountry, use_cache, urlpdb, peer_dict, origin_dict):
        if use_cache:
            try:
                infile = open(file_dictASNcountry, 'rb')
                dict_ASN_country = pickle.load(infile)
                infile.close()

                peeringdb_obj = PeeringdbPeer(urlpdb)

                infile = open(peer_dict, 'rb')
                peer_dict = pickle.load(infile)
                infile.close()

                infile = open(origin_dict, 'rb')
                origin_dict = pickle.load(infile)
                infile.close()

                peeringdbclean_obj = PeeringdbClean(origin_dict, peer_dict)
                flat_Origin_netfacs, flat_Origin_IXs = peeringdbclean_obj.iterate_originas_netfacs_IX()
                flat_peer_netfacs, flat_peer_IXs = peeringdbclean_obj.iterate_peeras_netfacs_IX()
                common_all_facs, common_all_IXs = peeringdbclean_obj.iterate_common(flat_peer_netfacs,
                                                                                    flat_Origin_netfacs, flat_peer_IXs,
                                                                                    flat_Origin_IXs)
                No_intersection = peeringdbclean_obj.iterate_intersection(common_all_IXs, common_all_facs)

                filename = 'D:/automate_Networks/No_intersection_PDB'  # final needed object
                outfile = open(filename, 'wb')
                pickle.dump(No_intersection, outfile)
                outfile.close()

                filename = 'D:/automate_Networks/flat_Origin_IXs'  # needed for file ixp_
                outfile = open(filename, 'wb')
                pickle.dump(flat_Origin_IXs, outfile)
                outfile.close()

            except FileNotFoundError:
                infile = open(file_dictASNcountry, 'rb')
                dict_ASN_country = pickle.load(infile)
                infile.close()

                print('Local files not found, Downloading files now...')
                peeringdb_obj = PeeringdbPeer(urlpdb)
                peer_dict = peeringdb_obj.get_peer_data(dict_ASN_country)  ## download data from pdb long time   D:\AS_objects\super_dict
                origin_dict = peeringdb_obj.iterate_originas_data(dict_ASN_country)  ### download data from pdb long time D:\AS_objects\Origins_ALLcountry

                peeringdbclean_obj = PeeringdbClean(origin_dict, peer_dict)

                flat_Origin_netfacs, flat_Origin_IXs = peeringdbclean_obj.iterate_originas_netfacs_IX()
                flat_peer_netfacs, flat_peer_IXs = peeringdbclean_obj.iterate_peeras_netfacs_IX()
                common_all_facs, common_all_IXs = peeringdbclean_obj.iterate_common(flat_peer_netfacs,
                                                                                    flat_Origin_netfacs, flat_peer_IXs,
                                                                                    flat_Origin_IXs)

                No_intersection = peeringdbclean_obj.iterate_intersection(common_all_IXs, common_all_facs)

                filename = 'D:/automate_Networks/flat_Origin_IXs'  # needed for file ixp_
                outfile = open(filename, 'wb')
                pickle.dump(flat_Origin_IXs, outfile)
                outfile.close()

                filename = 'D:/automate_Networks/No_intersection_PDB'  # final needed object
                outfile = open(filename, 'wb')
                pickle.dump(No_intersection, outfile)
                outfile.close()

                filename = 'D:/automate_Networks/peer_dict'  # final needed object
                outfile = open(filename, 'wb')
                pickle.dump(peer_dict, outfile)
                outfile.close()

                filename = 'D:/automate_Networks/origin_dict'  # final needed object
                outfile = open(filename, 'wb')
                pickle.dump(origin_dict, outfile)
                outfile.close()

        else:
            print('Downloading Files Now.....')
            infile = open(file_dictASNcountry, 'rb')
            dict_ASN_country = pickle.load(infile)
            infile.close()

            peeringdb_obj = PeeringdbPeer(urlpdb)
            peer_dict = peeringdb_obj.get_peer_data(dict_ASN_country)  ## download data from pdb long time   D:\AS_objects\super_dict
            origin_dict = peeringdb_obj.iterate_originas_data(dict_ASN_country)  ### download data from pdb long time D:\AS_objects\Origins_ALLcountry

            peeringdbclean_obj = PeeringdbClean(origin_dict, peer_dict)

            flat_Origin_netfacs, flat_Origin_IXs = peeringdbclean_obj.iterate_originas_netfacs_IX()
            flat_peer_netfacs, flat_peer_IXs = peeringdbclean_obj.iterate_peeras_netfacs_IX()
            common_all_facs, common_all_IXs = peeringdbclean_obj.iterate_common(flat_peer_netfacs,
                                                                                flat_Origin_netfacs, flat_peer_IXs,
                                                                                flat_Origin_IXs)

            No_intersection = peeringdbclean_obj.iterate_intersection(common_all_IXs, common_all_facs)
## make single function
            filename = 'D:/automate_Networks/flat_Origin_IXs'  # needed for file ixp_
            outfile = open(filename, 'wb')
            pickle.dump(flat_Origin_IXs, outfile)
            outfile.close()

            filename = 'D:/automate_Networks/No_intersection_PDB'  # final needed object
            outfile = open(filename, 'wb')
            pickle.dump(No_intersection, outfile)
            outfile.close()

            filename = 'D:/automate_Networks/peer_dict'  # final needed object
            outfile = open(filename, 'wb')
            pickle.dump(peer_dict, outfile)
            outfile.close()

            filename = 'D:/automate_Networks/origin_dict'  # final needed object
            outfile = open(filename, 'wb')
            pickle.dump(origin_dict, outfile)
            outfile.close()


class caida_ixp_data:

    def caida_ixp_cln(self, file_dictASNcountry, path_ixptoasn, path_ix):
        infile = open(file_dictASNcountry, 'rb')
        dict_ASN_country = pickle.load(infile)
        infile.close()

        caidaIxp_obj = CaidaIxp(path_ixptoasn, path_ix)
        ix_to_ASN, ixs_caida = caidaIxp_obj.get_data()

        caidaixpcleaner_obj = caidaIxpCleaner(ix_to_ASN, ixs_caida)
        asn_to_ix_grouped = caidaixpcleaner_obj.group_ix_to_asn()

        AS_2_IXs_caidaid = caidaixpcleaner_obj.extracter_oasn(dict_ASN_country, asn_to_ix_grouped)
        country_mains = caidaixpcleaner_obj.extracter_peer(dict_ASN_country, asn_to_ix_grouped)
        common_all_facs = caidaixpcleaner_obj.iterate_common_ix(country_mains, AS_2_IXs_caidaid)
        no_common_ALL = caidaixpcleaner_obj.iterate_find_nocommon(dict_ASN_country, common_all_facs)

        filename = 'D:/automate_Networks/no_intersection_CAIDA'
        outfile = open(filename, 'wb')
        pickle.dump(no_common_ALL, outfile)
        outfile.close()


class ixp_lc_pdb:

    def ixp_lc_cln(self, use_cache, file_ALLixp, urlpdb_loc, geojson_loc, flat_Origin_IXs):
        if use_cache:
            try:
                infile = open(file_ALLixp, 'rb')
                ALL_ixp = pickle.load(infile)
                infile.close()

                ixppdb_obj = IxpPeeringdb(urlpdb_loc)
                geo_data_cities = ixppdb_obj.geo_json_reader(geojson_loc)

                ixploc_cleaner_obj = IxpLocationCleaner(ALL_ixp)
                ALL_ixp_loc_withcountry = ixploc_cleaner_obj.extract_ixp_loc()
                ALL_ixp_loc_withcountry = ixploc_cleaner_obj.clean_city_str(ALL_ixp_loc_withcountry)

                city_country_unique = ixploc_cleaner_obj.unique_pair_citycountry(ALL_ixp_loc_withcountry)
                geo_Coords_city = ixploc_cleaner_obj.geojson_mapper(geo_data_cities, city_country_unique)

                ixploc_cleaner_obj.mapping(ALL_ixp_loc_withcountry, geo_Coords_city)

            except FileNotFoundError:
                print('Local files not found, Downloading files now...')
                infile = open(flat_Origin_IXs, 'rb')
                flat_Origin_IXs = pickle.load(infile)
                infile.close()

                ixppdb_obj = IxpPeeringdb(urlpdb_loc)
                ALL_ixp = ixppdb_obj.get_ixp_data(flat_Origin_IXs)
                geo_data_cities = ixppdb_obj.geo_json_reader(geojson_loc)

                ixploc_cleaner_obj = IxpLocationCleaner(ALL_ixp)
                ALL_ixp_loc_withcountry = ixploc_cleaner_obj.extract_ixp_loc()
                ALL_ixp_loc_withcountry = ixploc_cleaner_obj.clean_city_str(ALL_ixp_loc_withcountry)

                city_country_unique = ixploc_cleaner_obj.unique_pair_citycountry(ALL_ixp_loc_withcountry)
                geo_Coords_city = ixploc_cleaner_obj.geojson_mapper(geo_data_cities, city_country_unique)

                ixploc_cleaner_obj.mapping(ALL_ixp_loc_withcountry, geo_Coords_city)

                filename = 'D:/automate_Networks/ALL_ixp'
                outfile = open(filename, 'wb')
                pickle.dump(ALL_ixp, outfile)
                outfile.close()

        else:
            print('Downloading files now...')
            infile = open(flat_Origin_IXs, 'rb')
            flat_Origin_IXs = pickle.load(infile)
            infile.close()

            ixppdb_obj = IxpPeeringdb(urlpdb_loc)
            ALL_ixp = ixppdb_obj.get_ixp_data(flat_Origin_IXs)
            geo_data_cities = ixppdb_obj.geo_json_reader(geojson_loc)

            ixploc_cleaner_obj = IxpLocationCleaner(ALL_ixp)
            ALL_ixp_loc_withcountry = ixploc_cleaner_obj.extract_ixp_loc()
            ALL_ixp_loc_withcountry = ixploc_cleaner_obj.clean_city_str(ALL_ixp_loc_withcountry)

            city_country_unique = ixploc_cleaner_obj.unique_pair_citycountry(ALL_ixp_loc_withcountry)
            geo_Coords_city = ixploc_cleaner_obj.geojson_mapper(geo_data_cities, city_country_unique)

            ixploc_cleaner_obj.mapping(ALL_ixp_loc_withcountry, geo_Coords_city)

            filename = 'D:/automate_Networks/ALL_ixp'
            outfile = open(filename, 'wb')
            pickle.dump(ALL_ixp, outfile)
            outfile.close()


class ixp_perc_map:

    def ixp_prcmapper(self, file_dictASNcountry, path_ixptoasn, path_ix, file_ALLixp, geojson_loc):
        infile = open(file_dictASNcountry, 'rb')
        dict_ASN_country = pickle.load(infile)
        infile.close()

        caidaIxp_obj = CaidaIxp(path_ixptoasn, path_ix)
        ix_to_ASN, ixs_caida = caidaIxp_obj.get_data()

        map_obj = IxpMapPlotting(ixs_caida)
        caidaixp_dict = map_obj.ixpdict()

        caidaixpcleaner_obj = caidaIxpCleaner(ix_to_ASN, ixs_caida)
        asn_to_ix_grouped = caidaixpcleaner_obj.group_ix_to_asn()
        AS_2_IXs_caidaid = caidaixpcleaner_obj.extracter_oasn(dict_ASN_country, asn_to_ix_grouped)

        sab = map_obj.transform_caidadict(AS_2_IXs_caidaid, caidaixp_dict)

        infile = open(file_ALLixp, 'rb')
        ALL_ixp = pickle.load(infile)
        infile.close()

        alles = map_obj.pdb_dict_transform(ALL_ixp)

        alles_consolidated = map_obj.consolidate_pdb_caida(alles, sab)

        alles_country_caidapdb = map_obj.perc_IXP_calc(AS_2_IXs_caidaid, alles_consolidated)

        geo_data_cities = map_obj.geo_json_reader(geojson_loc)

        map_obj.plot_IXPmap(geo_data_cities, alles_country_caidapdb)

        with open('D:/automate_Networks/IXs_locs_pdb', 'wb') as outfile:           #needed for heatmap plotting
            pickle.dump(alles,outfile)


class ixp_heatmap_allcountry:

    def ixp_heatmapper(self,file_IXs_locs_pdb,file_ALLixp,json_1,json_2):

        with open(file_IXs_locs_pdb, 'rb') as infile:
            IXs_locs_pdb = pickle.load(infile)

        with open(file_ALLixp, 'rb') as infile:
            ALL_ixp = pickle.load(infile)

        hm_obj = AllHeatmap(IXs_locs_pdb, ALL_ixp)
        heatmap = hm_obj.Isp_counter()
        heatmap2 = hm_obj.Isp_perc(heatmap)
        geo_country_dict = hm_obj.geo_json_reader(json_1)
        geo_Coords_city = hm_obj.coordinate_mapper(geo_country_dict, heatmap2)
        hm_obj.heatmapping(heatmap2, geo_Coords_city, json_2)

        with open('D:/automate_Networks/heatmap', 'wb') as outfile:  # needed for uniq country heatmaps plotting
            pickle.dump(heatmap, outfile)


class ixp_hm_uc:

    def ixp_hm_ucmapper(self,file_IXs_locs_pdb,file_heatmap,json_2):

        with open(file_IXs_locs_pdb, 'rb') as infile:
            IXs_locs_pdb = pickle.load(infile)

        with open(file_heatmap, 'rb') as infile:
            heatmap = pickle.load(infile)

        hm_uc_obj = IxpHeatmapUniqueCountry(IXs_locs_pdb)
        noncountry = hm_uc_obj.find_out_of_home_country()
        outer = hm_uc_obj.unique_country_finder(noncountry)
        mains = hm_uc_obj.perc_uc(outer, heatmap)
        hm_uc_obj.hm_plotter(mains, json_2)




if __name__ == '__main__':
    config = configparser.ConfigParser()
    location = Path(__file__).resolve().parent.parent.joinpath('config', 'default.ini')
    config.read(location)

    print('Executing caida_peer')
    caida_peer().caida_cleaner(file_rawdata=config.get('caida_relation_and_peer', 'rawdata_path'),
                               use_cache=config.getint('caida_relation_and_peer', 'use_cache'),
                               asrank_url=config.get('caida_relation_and_peer', 'asrank_url'),
                               maxpage=config.getint('caida_relation_and_peer', 'maxpage'),
                               caidapeer_file=config.get('caida_relation_and_peer', 'caidapeer_file'))

    print('Executing pdb_cleaner')
    pdb().pdb_cleaner(file_dictASNcountry=config.get('peeringdb_data', 'file_dictASNcountry'),
                      use_cache=config.getint('peeringdb_data', 'use_cache'),
                      urlpdb=config.get('peeringdb_data', 'urlpdb'),
                      peer_dict=config.get('peeringdb_data', 'peer_dict'),
                      origin_dict=config.get('peeringdb_data', 'origin_dict'))

    print('Executing caida_ixp_cln')
    caida_ixp_data().caida_ixp_cln(file_dictASNcountry=config.get('caidaixpdata', 'file_dictASNcountry'),
                                   path_ixptoasn=config.get('caidaixpdata', 'path_ixptoasn'),
                                   path_ix=config.get('caidaixpdata', 'path_ix'))

    print('Executing ixp_lc_cln')

    ixp_lc_pdb().ixp_lc_cln(use_cache=config.getint('ixplocation_pdb', 'use_cache'),
                            file_ALLixp=config.get('ixplocation_pdb', 'file_ALLixp'),
                            urlpdb_loc=config.get('ixplocation_pdb', 'urlpdb_loc'),
                            geojson_loc=config.get('ixplocation_pdb', 'geojson_loc'),
                            flat_Origin_IXs=config.get('ixplocation_pdb', 'flat_Origin_IXs'))

    print('Executing ixp_prcmapper')
    ixp_perc_map().ixp_prcmapper(file_dictASNcountry=config.get('ixplocationconsolidated', 'file_dictASNcountry'),
                                 path_ixptoasn=config.get('ixplocationconsolidated', 'path_ixptoasn'),
                                 path_ix=config.get('ixplocationconsolidated', 'path_ix'),
                                 file_ALLixp=config.get('ixplocationconsolidated', 'file_ALLixp'),
                                 geojson_loc=config.get('ixplocationconsolidated', 'geojson_loc'))

    print('Executing ixp_heatmap_allcountry')
    ixp_heatmap_allcountry().ixp_heatmapper(file_IXs_locs_pdb= config.get('ixp_heatmap_allcountry','file_IXs_locs_pdb'),
                                            file_ALLixp=config.get('ixp_heatmap_allcountry','file_ALLixp'),
                                            json_1=config.get('ixp_heatmap_allcountry','json1'),
                                            json_2=config.get('ixp_heatmap_allcountry','json2'))

    print('Executing ixp_hm_uc')
    ixp_hm_uc().ixp_hm_ucmapper(file_IXs_locs_pdb=config.get('ixp_hm_uc', 'file_IXs_locs_pdb'),
                                            file_heatmap=config.get('ixp_hm_uc', 'file_heatmap'),
                                            json_2=config.get('ixp_hm_uc', 'json2'))


    print('ALL Done :)')
