from decimal import Decimal
import pandas as pd
import urllib.request as urlrequest
import json
import os
import sys
import shutil
import time
import datetime
import logging

apikey="F9PB22XVU41P78AVBY3RZEK8EQMXE4HAGN"     # 这个换成自己账户的apikey
TXN_DF_COLUMN_NAMES = ['TxHash', 'BlockHeight', 'TimeStamp', 'From', 'To', 'Value', 'ContractAddress', 'Input', 'isError']
TXN_FIELD_NAMES = ['hash', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'contractAddress', 'input', 'isError']
OUTPUT_DIR = f'results-[{(datetime.datetime.now()).strftime("%m%d%H%M%S")}]/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

K = 2
node_pardir_dict = {}   # one-to-one mapping b/w node(addr) and its parent directory

time_ = time.time()
def main():
    # setup_logger(OUTPUT_DIR)
    queue = get_original_nodes()
    for original_node in queue:
        node_pardir_dict[original_node] = OUTPUT_DIR
    while queue:
        queue = process_head_node(queue)
    save_node_hash()
    # analyze_graph()


def process_head_node(queue):
    """
    node = queue.pop(0)
    assert node in node_pardir_dict
    if order_of(node) == K:
        return
    neighbors = get_neighbors(node)
    for neighbor_node in neighbors:
        if neighbor_node not in node_pardir_dict:

            queue.append(neighbor_node)
    :return:
    """
    node = queue.pop(0)
    assert node in node_pardir_dict
    if order_of(node) == K:
        return
    cur_dir = os.path.join(node_pardir_dict[node], node)
    os.makedirs(cur_dir, exist_ok=True)
    txns = get_txns_of_node(node)
    if len(txns) >= 10000:
        return
    txn_df = pd.DataFrame(columns=TXN_DF_COLUMN_NAMES)
    neighbor_set = set()
    for txn in txns:
        if is_valid_txn(txn):
            txn_df = txn_df.append(txn2pdseries(txn), ignore_index=True)
            neighbor = txn['from'].lower() if txn['from'].lower() != node else txn['to'].lower()
            if neighbor in node_pardir_dict:
                continue
            neighbor_set.add(neighbor)
    for neighbor in neighbor_set:
        queue.append(neighbor)
        node_pardir_dict[neighbor] = cur_dir
    txn_df.to_csv(os.path.join(cur_dir, f'txns.csv'))
    df_neighbor = pd.DataFrame(data=list(neighbor_set), columns=['node'])
    df_neighbor.to_csv(os.path.join(cur_dir, f'neighbors.csv'))
    return queue


def get_original_nodes():
    nodes = [
        # low risk
        '0xfa171c2a5BB16cD608Ce3aC7A8e8C1e4B554EcBE',
        '0xd1707D1696cEE3254878bd81b0aE3b7252A06B6e',
        '0x4F71D67322f7f97944c26A917acD990b793E0f2A',
        '0xBE38a889D67467b665E30E20eE5604A6F5696e38',
        '0xcDE1250f112Ac69Ae5f7D561Ad052816476Fc6d1',
        # high risk
        '0x0b7f284d74f549731499c44aed2a10adcc9e9cc0',
        '0xF6884686a999f5ae6c1AF03DB92BAB9c6d7DC8De',
        '0xDf9191889649C442836ef55De5036a7b694115b6',
        '0x2664c334c46635f7845487d3BAb16992Fc83A93e',
        '0x1f6f1723d0db4e9783b7171392b6fa9ae1062fd9',
    ]
    nodes_lower = []
    for add in nodes:
        nodes_lower.append(add.lower())
    return nodes_lower


def get_txns_of_node(node):
    url = f'http://api.etherscan.io/api?module=account&action=txlist&address={node}&' \
                   f'startblock=0&endblock=99999999&sort=asc&apikey={apikey}'
    txn_json = json.loads(urlrequest.urlopen(url).read().decode('utf8'))
    txns = txn_json['result'] if txn_json['status'] == '1' else []

    url_internal = f'http://api.etherscan.io/api?module=account&action=txlistinternal&address={node}&' \
                   f'startblock=0&endblock=99999999&sort=asc&apikey={apikey}'
    txn_json_internal = json.loads(urlrequest.urlopen(url_internal).read().decode('utf8'))
    txns_internal = txn_json_internal['result'] if txn_json_internal['status'] == '1' else []
    txns += txns_internal
    return txns


def is_valid_txn(txn):
    if txn['value'] == '0':
        return False
    if txn['isError'] == '1':
        return False
    if txn['from'].lower() == txn['to'].lower(): # is this invalid?
        # print(txn['from'].lower())
        return False
    return True


def txn2pdseries(txn):
    for k, v in txn.items():
        if v == "":
            txn[k] = 'NULL'
        if k == 'value':
            txn[k] = wei2ether(v)

    s = {}
    for i in range(len(TXN_DF_COLUMN_NAMES)):
        s[TXN_DF_COLUMN_NAMES[i]] = txn[TXN_FIELD_NAMES[i]]
    return pd.Series(s)


def wei2ether(s):
    length = len(s)
    t = length - 18
    if t > 0:
        s1 = ""
        s1 = s1+s[0:t]
        s1 = s1+"."
        s1 = s1+s[t:]
    else:
        x = 18-length
        s1 = "0."
        for i in range(0,x):
            s1 = s1+"0"
        s1 = s1+s
    return Decimal(s1)


def save_node_hash():
    print(len(node_pardir_dict))
    HASH_LEN = 10000
    hash_dict = {}
    for i in range(HASH_LEN):
        hash_dict[i] = []
    for node in node_pardir_dict.keys():
        hash_idx = int(node, 16) % HASH_LEN
        hash_dict[hash_idx].append(node)
    with open(os.path.join(OUTPUT_DIR, 'node_hash.txt'), 'w') as f:
        for i in range(HASH_LEN):
            for node in hash_dict[i]:
                f.write(node + ' ')
            f.write('\n')


def order_of(node):
    """
    #node whose pardir is 'results-[1004015004]/0x0059b14e35dab1b4eee1e2926c7a5660da66f747/' is ordered 0
    :param node:
    :return:
    """
    order = len(node_pardir_dict[node].strip('/').split('/')) - len(OUTPUT_DIR.strip('/').split('/')) - 1
    return order


def setup_logger(output_dir, distributed_rank=0):
    # ---- make output dir ----
    # each experiment's output is in the dir named after the time when it starts to run
    # log_dir_name = log_prefix + '-[{}]'.format((datetime.datetime.now()).strftime('%m%d%H%M%S'))
    # log_dir_name += cfg.EXPERIMENT_NAME
    os.makedirs(output_dir, exist_ok=True)

    # ---- set up logger ----
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # don't log results for the non-master process
    if distributed_rank > 0:
        return logger
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: %(message)s", '%m%d%H%M%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    txt_name = 'log.txt'
    for i in range(2, 100000):
        if os.path.exists(os.path.join(output_dir, txt_name)):
            txt_name = f'log-{i}.txt'
        else:
            break
    fh = logging.FileHandler(os.path.join(output_dir, txt_name), mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


def log_info(log_str):
    logger = logging.getLogger()
    if len(logger.handlers):
        logger.info(log_str)
    else:
        print(log_str)




if __name__ == '__main__':
    main()