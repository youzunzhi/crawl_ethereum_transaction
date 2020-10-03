from decimal import Decimal
import pandas as pd
import urllib.request as urlrequest
import json
import os
import datetime
import time

apikey="F9PB22XVU41P78AVBY3RZEK8EQMXE4HAGN" #这个换成自己账户的apikey
TXN_DF_COLUMN_NAMES = ['TxHash', 'BlockHeight', 'TimeStamp', 'From', 'To', 'Value', 'ContractAddress', 'Input', 'isError']
TXN_FIELD_NAMES = ['hash', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'contractAddress', 'input', 'isError']
OUTPUT_DIR = f'results-[{(datetime.datetime.now()).strftime("%m%d%H%M%S")}]/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

time_ = time.time()
def main():
    node_set = set()
    addresses = get_addresses()
    for address in addresses:
        node_set = get_k_order_neighbor(node_set, OUTPUT_DIR, address, cur_order=0, k=2)
    print_node_hash(node_set, OUTPUT_DIR)
    # analyze_graph()


def get_addresses():
    addresses = [
        '0x0059b14e35daB1b4EEe1e2926C7A5660dA66F747',
        '0x008f3db10374099a11ec263415cb88c952abeedc',
        '0x00e01A648Ff41346CDeB873182383333D2184dd1',
        '0x020b1573f2ca670190d33ca2f0a57b0c0399ad37',
        '0x052Ee5470868D14b158abb10DfcC76eD1cD1293d',
    ]
    return addresses


def get_k_order_neighbor(node_set, cur_dir, address, cur_order, k):
    if k == cur_order - 1:
        return

    node_set.add(address)
    cur_dir = os.path.join(cur_dir, address)
    os.makedirs(cur_dir, exist_ok=True)
    print(f"{cur_order}-order@{cur_dir}, {time.time()-time_}")

    txns = get_txns_from_address(address)
    txn_df = pd.DataFrame(columns=TXN_DF_COLUMN_NAMES)
    neighbor_set = set()
    for txn in txns:
        if is_valid_txn(txn):
            txn_df = txn_df.append(txn2pdseries(txn), ignore_index=True)
            # neighbor_from = txn['from'].lower()
            # neighbor_to = txn['to'].lower()
            # if neighbor_from != address:
            #     neighbor_set.add(neighbor_from)
            #     get_k_order_neighbor(node_set, cur_dir, neighbor_from, cur_order+1, k)
            # elif neighbor_to != address:
            #     neighbor_set.add(neighbor_to)
            #     get_k_order_neighbor(node_set, cur_dir, neighbor_to, cur_order+1, k)
            neighbor = txn['from'].lower() if txn['from'].lower()!=address else txn['to'].lower()
            neighbor_set.add(neighbor)
            if neighbor not in cur_dir.split('/'):
                get_k_order_neighbor(node_set, cur_dir, neighbor, cur_order+1, k)

    txn_df.to_csv(os.path.join(cur_dir, 'txns.csv'))
    df_neighbor = pd.DataFrame(data=list(neighbor_set), columns=['address'])
    df_neighbor.to_csv(os.path.join(cur_dir, 'neighbors.csv'))
    return node_set


def get_txns_from_address(address):
    url = f'http://api.etherscan.io/api?module=account&action=txlist&address={address}&' \
                   f'startblock=0&endblock=99999999&sort=asc&apikey={apikey}'
    txn_json = json.loads(urlrequest.urlopen(url).read().decode('utf8'))
    txns = txn_json['result'] if txn_json['status'] == '1' else []

    url_internal = f'http://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&' \
                   f'startblock=0&endblock=99999999&sort=asc&apikey={apikey}'
    txn_json_internal = json.loads(urlrequest.urlopen(url_internal).read().decode('utf8'))
    txns_internal = txn_json_internal['result'] if txn_json_internal['status'] == '1' else []
    txns += txns_internal
    return txns


def is_valid_txn(txn):
    if txn['value'] != '0':
        return False
    if txn['isError'] != '1':
        return False
    if txn['from'].lower() == txn['to'].lower():
        print(txn['from'].lower())
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


def print_node_hash(node_set, output_dir):
    print(len(node_set))
    HASH_LEN = 1000
    hash_dict = {}
    for i in range(HASH_LEN):
        hash_dict[i] = []
    for node in node_set:
        hash_idx = int(node, 16) % 100
        hash_dict[hash_idx].append(node)
    with open(os.path.join(output_dir, 'node_hash.txt')) as f:
        for i in range(HASH_LEN):
            for node in hash_dict[i]:
                f.write(node + ' ')
            f.write('\n')


if __name__ == '__main__':
    main()