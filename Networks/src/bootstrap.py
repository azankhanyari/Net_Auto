# import src
# import os
#
# print(src.base_path)
#
# defaultconfig = os.path.join(src.base_path,'config','default.ini')
# print(defaultconfig)
#
#

from pathlib import Path

base = (Path(__file__).resolve().parent).joinpath('config','default.ini')
print(base)

import argparse

parser = argparse.ArgumentParser()
# parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default = defaultconfig)

parser.add_argument("-C","--config_file", help="Get all parameters from the specified config file", type=str, default = base)
