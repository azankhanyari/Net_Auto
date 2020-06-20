import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


class CaidaPeer:
    def __init__(self, path: str):
        self.path = path

    def get_data(self):
        all_data = []
        with open(self.path, 'r') as f:
            for line in f.readlines()[180:]:
                all_data.append(line.strip().split('|'))

        all_data = pd.DataFrame(all_data, columns=['AS', 'peer', 'is_peer'])
        all_data['is_peer'] = pd.to_numeric(all_data['is_peer'])
        all_data['is_peer'] += 1

        return all_data


if __name__ == '__main__':
    caida_obj = CaidaPeer('C:/Users/Azaan/Downloads/Compressed/20200401.as-rel.txt/20200401.as-rel.txt')
    print(caida_obj.get_data())
