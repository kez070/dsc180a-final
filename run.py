import sys
import json
import shutil
sys.path.insert(0, 'src')

from etl import parse_unzipped
from etl import unzip
from etl import convert_to_light
from etl import calculate_M
import glob

DATA_PARAMS = 'config/data-params.json'
TEST_PARAMS = 'config/test-params.json'


def load_params(fp):
    with open(fp) as fh:
        param = json.load(fh)

    return param


def main(targets):

    # make the clean target
    if 'unzip' in targets:
        fp = load_params(DATA_PARAMS)['fp']
        output = load_params(DATA_PARAMS)['output']
        unzip(fp,output)

    # make the data target
    if 'parse_unzipped' in targets:
        fp = glob.glob(load_params(DATA_PARAMS)['output']+"/*")[0]
        parse_unzipped(fp)

    if 'clean' in targets:
        convert_to_light()
    
    if 'calculate_m' in targets:

        calculate_M()

    if 'test-project' in targets:
        fp = load_params(TEST_PARAMS)['fp']
        output = load_params(TEST_PARAMS)['output']
        unzip(fp,output)
        fp = glob.glob(load_params(TEST_PARAMS)['output']+"/*")[0]
        parse_unzipped(fp)
        convert_to_light()
        calculate_M()

    return


if __name__ == '__main__':
    targets = sys.argv[1:]
    main(targets)

