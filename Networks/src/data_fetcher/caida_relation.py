import json
import urllib
import urllib.request



class AsRelationship:
    def __init__(self, caida_url: str, max_page: int):
        self.caida_url = caida_url
        self.max_page = max_page

    def get_rank_data(self, page_num: int = 1):
        rawdata = []
        error = []
        while page_num < self.max_page:
            try:
                print('Downloading page no= %s' %page_num)
                resp = urllib.request.urlopen(self.caida_url % page_num)
                rawdata += json.loads(resp.read())['data']
            except Exception:
                error.append(page_num)
            page_num += 1

        return dict(data=rawdata, error=error)


if __name__ == '__main__':
    obj = AsRelationship('http://as-rank-test.caida.org/api/v1/asns?populate=1&page=%s&sort=customer_cone_addresses', 3)
    print(obj.get_rank_data())
