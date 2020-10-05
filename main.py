from decimal import Decimal
import pandas as pd
import urllib.request as urlrequest
import json
import os
import shutil
import time
import datetime

apikey="F9PB22XVU41P78AVBY3RZEK8EQMXE4HAGN"     # 这个换成自己账户的apikey
TXN_DF_COLUMN_NAMES = ['TxHash', 'BlockHeight', 'TimeStamp', 'From', 'To', 'Value', 'ContractAddress', 'Input', 'isError']
TXN_FIELD_NAMES = ['hash', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'contractAddress', 'input', 'isError']
OUTPUT_DIR = f'results-[{(datetime.datetime.now()).strftime("%m%d%H%M%S")}]/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

K = 2
node_pardir_dict = {}   # one-to-one mapping b/w node(addr) and its parent directory

time_ = time.time()
def main():
    original_nodes = get_original_nodes()
    for node in original_nodes:
        get_k_order_neighbors_txns(OUTPUT_DIR, node, cur_order=-1)
    print_node_hash()
    # analyze_graph()


def get_original_nodes():
    nodes = [
        '0x0059b14e35daB1b4EEe1e2926C7A5660dA66F747',
        '0x7E054e0F3153caDbe040e325099a6E1d7EB4454a',
    ]
    nodes_lower = []
    for add in nodes:
        nodes_lower.append(add.lower())
    return nodes_lower


# def process_neighbor(neighbor):
#     if neighbor in node_pardir_dict:
#         old_order = get_order_of_searched_node(neighbor)
#         if old_order > cur_order + 1:
#             if old_order < K:  # which means it has its own dir
#                 shutil.move(os.path.join(node_pardir_dict[neighbor], node), cur_dir)
#                 node_pardir_dict[neighbor] = cur_dir
#                 # 进入到这里说明neighbor是搜过的，应该避免重复再搜一遍。
#             get_k_order_neighbor(cur_dir, neighbor, cur_order + 1)
#     else:
#         get_k_order_neighbor(cur_dir, neighbor, cur_order + 1)

def get_k_order_neighbors_txns(pardir, node, cur_order):
    """
    if cur_order == K:
        if node没被搜过：
            node_pardir_dict[node] = pardir
        return
    if node被搜过且有文件夹：
        if node上次被搜时的阶数大于当前阶数：
            把node的文件夹移到pardir，也就是node应该在的pardir
            读取node的所有neighbor
            处理node的所有neighbor
        else node上次被搜时的阶数小于等于当前阶数：
            return
    else 1. node没被搜过 or 2. node被搜过但是没有文件夹
        node_pardir_dict[node] = pardir 1-加入dict，2-说明它之前是二阶，cur_dir是一阶及以下，它应该在pardir
        爬取node的所有neighbor
        处理node的所有neighbor

    :param pardir: node的上级文件夹
    :param node: node的address
    :param cur_order: 当前阶数
    :return:
    """
    if cur_order == K:
        if node not in node_pardir_dict:
            node_pardir_dict[node] = pardir
        return
    print(f"{cur_order}-order node {node} under {pardir}, {time.time()-time_:.1f}s")
    if node in node_pardir_dict and os.path.exists(os.path.join(node_pardir_dict[node], f'{node}/neighbors.csv')):
        if get_order_of_searched_node(node) > cur_order:
            print(f"moving {os.path.join(node_pardir_dict[node], node)} to {pardir}")
            shutil.move(os.path.join(node_pardir_dict[node], node), pardir)
            node_pardir_dict[node] = pardir
            neighbors = get_neighbors(node)
            for neighbor in neighbors:
                get_k_order_neighbors_txns(os.path.join(pardir, node), neighbor, cur_order + 1)
        else:
            return
    else:
        node_pardir_dict[node] = pardir
        txns = get_txns_of_node(node)
        if len(txns) >= 10000:
            return
        txn_df = pd.DataFrame(columns=TXN_DF_COLUMN_NAMES)
        neighbor_set = set()
        for txn in txns:
            if is_valid_txn(txn):
                txn_df = txn_df.append(txn2pdseries(txn), ignore_index=True)
                neighbor = txn['from'].lower() if txn['from'].lower() != node else txn['to'].lower()
                get_k_order_neighbors_txns(os.path.join(pardir, node), neighbor, cur_order + 1)
                neighbor_set.add(neighbor)
        os.makedirs(os.path.join(pardir, node), exist_ok=True)
        txn_df.to_csv(os.path.join(pardir, f'{node}/txns.csv'))
        df_neighbor = pd.DataFrame(data=list(neighbor_set), columns=['node'])
        df_neighbor.to_csv(os.path.join(pardir, f'{node}/neighbors.csv'))


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


def print_node_hash():
    print(len(node_pardir_dict))
    HASH_LEN = 1000
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


def get_order_of_searched_node(node):
    """
    #node whose pardir is 'results-[1004015004]/0x0059b14e35dab1b4eee1e2926c7a5660da66f747/' is ordered 0
    :param node:
    :return:
    """
    order = len(node_pardir_dict[node].strip('/').split('/')) - len(OUTPUT_DIR.strip('/').split('/')) - 1
    return order


def get_neighbors(node):
    neighbors = []
    with open(os.path.join(node_pardir_dict[node], f'{node}/neighbors.csv'), 'r') as f:
        l = f.readlines()
        for line in l[1:]:
            neighbors.append(line.split(',')[1].strip())
    return neighbors




if __name__ == '__main__':
    main()