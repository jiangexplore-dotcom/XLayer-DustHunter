#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xdog_quick_demo.py
快速样例版：只扫最新 N 个区块，分钟级出结果。
输出文件与原全量版完全兼容：
  xdog_edges.csv
  dust_report.json      → 实时追加版（jsonlines）
  real_holding_top.csv
  dust_graph.gexf       → 实时追加边（csv）
新增：轻量化断点续扫（单数字头），支持 --reset / --rewind / --full（第一次扫全链）
      ✅ 实时写边、头、余额、候选、粉尘堆、图边——全部逐块追加，Ctrl-C 只丢最后一条
"""
import os, json, time, functools, argparse, csv
from typing import Dict, List, Set
import pandas as pd
import networkx as nx
from tqdm import tqdm
from web3 import Web3
from eth_utils import to_checksum_address
from collections import defaultdict

# ---------- 用户配置 ----------
TOKEN_CONTRACT = Web3.to_checksum_address("")
RPC_URL        = "https://rpc.xlayer.tech"
SAMPLE_BLOCKS  = 67000_00                   # None=第一次扫全链；可改 2000 等
MAX_BLOCKS_ONCE= 100
BALANCE_THRESHOLD_WEI = 1000000 * 10**18   # 100 万枚
MAX_OUT_TX     = 2
CLUSTER_MIN_ADDR = 5
ENRICH_MIN     = 0.05
PRICE_USD      = 0.20
# 实时文件
CAND_FILE      = "dust_candidate.jsonl"    # 每地址一行
REPORT_FILE    = "dust_report.jsonl"       # 每堆一行
GRAPH_EDGE_FILE= "dust_graph.edges.csv"    # 实时边
# ------------------------------

w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 60}))
if not w3.is_connected():
    raise RuntimeError("RPC 无法连接")

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CACHE_FILE     = "cache_bal_nonce.json"
SCAN_HEAD_FILE = "scan_head.json"      # 断点文件：只存 {"head": 12345}

# ====================== 通用工具 ======================
def retry(func):
    @functools.wraps(func)
    def wrapper(*a, **kw):
        for i in range(5):
            try:
                return func(*a, **kw)
            except Exception as e:
                print(f"[retry {i+1}/5] {e}")
                time.sleep(2**i)
        raise
    return wrapper

@retry
def fetch_events(from_block: int, to_block: int):
    return w3.eth.get_logs({
        'fromBlock': from_block,
        'toBlock': to_block,
        'address': TOKEN_CONTRACT,
        'topics': [TRANSFER_TOPIC]
    })

def parse_event(evt):
    fr = to_checksum_address('0x' + evt['topics'][1].hex()[-40:])
    to = to_checksum_address('0x' + evt['topics'][2].hex()[-40:])
    data = evt['data']
    if isinstance(data, bytes):
        val = int.from_bytes(data, byteorder='big')
    else:
        val = int(data, 16)
    return fr, to, val

# ====================== 轻量化断点续扫框架 ======================
def _load_scan_head() -> int:
    if os.path.exists(SCAN_HEAD_FILE):
        return json.load(open(SCAN_HEAD_FILE)).get("head")
    return None

def _save_scan_head(head: int):
    with open(SCAN_HEAD_FILE, "w", encoding="utf-8") as f:
        json.dump({"head": head}, f, indent=2)
        os.fsync(f.fileno())          # 强制刷盘

def get_scan_range(latest: int, sample_blocks: int = SAMPLE_BLOCKS, rewind: int = 0, full: bool = False):
    if sample_blocks is None:
        sample_blocks = latest + 1 if full else SAMPLE_BLOCKS
    head = _load_scan_head()
    if head is None or rewind:
        if sample_blocks is None or full:
            sample_blocks = latest + 1
        head = (latest - sample_blocks) if head is None else head - rewind
        head = max(head, 0)
    start = head + 1
    end   = min(start + sample_blocks - 1, latest)
    if start > end:
        return None, None
    return start, end

def update_scan_head(end: int):
    _save_scan_head(end)

# ====================== 扫链（实时写边+实时写头） ======================
# ====================== 扫链（实时写边+头） ======================
def scan_sample_events():
    latest = w3.eth.block_number
    start, end = get_scan_range(latest, sample_blocks=SAMPLE_BLOCKS, full=args.full)
    if start is None:
        print("已追上最新块，无新区间可扫")
        return pd.DataFrame(columns=['from', 'to', 'value'])   # ← 空表也带列名

    print(f"断点续扫：{start} ~ {end}  （最新 {latest}）")
    csv_path = "xdog_edges.csv"
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["from", "to", "value"])

        for _from in tqdm(range(start, end + 1, MAX_BLOCKS_ONCE),
                          total=(end - start) // MAX_BLOCKS_ONCE + 1,
                          desc="Quick Scan"):
            _to = min(_from + MAX_BLOCKS_ONCE - 1, end)
            evts = fetch_events(_from, _to)
            for e in evts:
                fr, to, val = parse_event(e)
                if fr == to:
                    continue
                writer.writerow([fr, to, val])
            update_scan_head(_to)

    # 2. 空文件保护 + 列名统一
    if os.path.getsize(csv_path):
        df = pd.read_csv(csv_path)
    else:
        df = pd.DataFrame(columns=['from', 'to', 'value'])  # 0 行也带列名
    df.columns = df.columns.str.strip().str.lower().str.replace('"', '')  # 去空格/引号/小写
    return df

# ====================== 余额 & nonce（实时写候选） ======================
def load_cache() -> Dict[str, Dict[str, int]]:
    if os.path.exists(CACHE_FILE):
        return json.load(open(CACHE_FILE, encoding="utf-8"))
    return {}

def save_cache(cache: Dict[str, Dict[str, int]]):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

@retry
def _get_balance(a, contract, block):
    return contract.functions.balanceOf(a).call(block_identifier=block)

@retry
def _get_nonce(a, block):
    return w3.eth.get_transaction_count(a, block_identifier=block)

def batch_balance_and_nonce(all_addrs: Set[str]):
    abi = [{"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"}]
    contract = w3.eth.contract(address=TOKEN_CONTRACT, abi=abi)
    latest_safe = w3.eth.block_number
    cache = load_cache()
    todo = list(all_addrs - set(cache.keys()))
    print(f"缓存命中 {len(cache)} 条，剩余 {len(todo)} 条待查询")

    # 实时候选文件
    from jsonlines import open as jopen
    with jopen(CAND_FILE, 'a') as cf:
        for a in tqdm(todo, desc="Balance&Nonce"):
            try:
                bal = _get_balance(a, contract, latest_safe)
                nonce = _get_nonce(a, latest_safe)
                cache[a] = {"bal": bal, "nonce": nonce}
                save_cache(cache)
                # 🔥实时写候选
                if bal <= BALANCE_THRESHOLD_WEI and nonce <= MAX_OUT_TX + 1:
                    cf.write({"addr": a, "bal": bal, "nonce": nonce})
            except Exception as e:
                print(f"\n{a} 查询失败: {e}，已跳过")
                continue

    # 统一返回格式
    bal_map, nonce_map = {}, {}
    for a in all_addrs:
        bal_map[a]   = cache[a]["bal"]
        nonce_map[a] = cache[a]["nonce"]
    return bal_map, nonce_map

# ====================== 粉尘堆检测（实时追加） ======================
def detect_dust_clusters(df_edges, bal_map, nonce_map):
        # 列名修复 & 空表保护
    if df_edges.empty:
        print("df_edges 为空，跳过聚类")
        return {}
    df_edges.columns = df_edges.columns.str.strip().str.lower()  # 统一小写
    for col in ['from', 'to', 'value']:
        if col not in df_edges.columns:
            print(f"缺列 {col}，跳过聚类")
            return {}
    # 只读 **新候选地址**
    from jsonlines import open as jopen
    new_cand = [ln["addr"] for ln in jopen(CAND_FILE)] if os.path.exists(CAND_FILE) else []
    if not new_cand:
        return {}

    G = nx.DiGraph()
    # 简化为全量读边（可优化为只读新边）
    for _, r in df_edges.iterrows():
        G.add_edge(r['from'], r['to'], value=int(r['value']))

    parent = defaultdict(set)
    for _, r in df_edges.iterrows():
        src, dst = r['from'], r['to']
        if dst in new_cand:
            parent[dst].add(src)

    cluster = defaultdict(list)
    for addr in new_cand:
        if len(parent[addr]) == 1:
            upper = list(parent[addr])[0]
            cluster[upper].append(addr)

    total_addr = len(new_cand)
    total_bal  = sum(bal_map[a] for a in new_cand)

    # 实时追加模式写报告 & 图边
    with jopen(REPORT_FILE, 'a') as rf, open(GRAPH_EDGE_FILE, 'a', newline='') as ge:
        writer = csv.writer(ge)
        if not os.path.exists(GRAPH_EDGE_FILE):
            writer.writerow(["upper", "child"])          # 表头只写一次
        for upper, children in cluster.items():
            c_bal = sum(bal_map[a] for a in children)
            enrich = (len(children) / max(total_addr, 1)) * (c_bal / max(total_bal, 1))
            if len(children) >= CLUSTER_MIN_ADDR and enrich >= ENRICH_MIN:
                rf.write({
                    "upper": upper,
                    "children_cnt": len(children),
                    "children_balance": str(c_bal),
                    "enrich_ratio": enrich,
                    "children": children
                })
                for ch in children:
                    writer.writerow([upper, ch])         # 实时写图边

    # 返回全量 report（供后续排行用）
    report = {}
    if os.path.exists(REPORT_FILE):
        for ln in jopen(REPORT_FILE):
            report[ln["upper"]] = {
                "children_cnt": ln["children_cnt"],
                "children_balance": ln["children_balance"],
                "enrich_ratio": ln["enrich_ratio"],
                "children": ln["children"]
            }
    return report

# ====================== 真实持仓排行 ======================
def real_holding_rank(df_edges, bal_map, dust_report):
    real = []
    for up, meta in dust_report.items():
        children = meta["children"]
        total_bal = bal_map.get(up, 0) + sum(bal_map.get(a, 0) for a in children)
        outflow = 0
        for _, r in df_edges.iterrows():
            if r['from'] == up and r['to'] not in children:
                outflow += int(r['value'])
        real.append((up, total_bal, outflow, len(children)))
    handled = set(dust_report.keys())
    dust_addrs = {a for meta in dust_report.values() for a in meta["children"]}
    for a in bal_map:
        if a in handled or a in dust_addrs:
            continue
        real.append((a, bal_map[a], 0, 0))
    real.sort(key=lambda x: x[1], reverse=True)
    df = pd.DataFrame(real, columns=['address', 'real_balance_wei', 'outflow_wei', 'dust_count'])
    df['real_balance'] = df['real_balance_wei'] / 10 ** 18
    df['outflow'] = df['outflow_wei'] / 10 ** 18
    df['rank'] = df['real_balance'].rank(method='min', ascending=False).astype(int)
    df['value_usd'] = df['real_balance'] * PRICE_USD
    df[['rank', 'address', 'real_balance', 'value_usd', 'outflow', 'dust_count']].to_csv(
        "real_holding_top.csv", index=False, float_format='%.6f')
    print("\n====== 真实持仓 TOP-20 ======")
    print(df.head(20)[['rank', 'address', 'real_balance', 'value_usd', 'dust_count']])
    return df

# ====================== main ======================
def main():
    global args
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="删除所有断点与 csv，从头全量")
    ap.add_argument("--rewind", type=int, default=0, help="强制向前回退 N 块再开始扫")
    ap.add_argument("--full", action="store_true", help="第一次扫全链（等价 SAMPLE_BLOCKS=None）")
    args = ap.parse_args()

    if args.reset:
        for f in [SCAN_HEAD_FILE, "xdog_edges.csv", CAND_FILE, REPORT_FILE, GRAPH_EDGE_FILE]:
            os.remove(f) if os.path.exists(f) else None
        print("已清除所有实时文件，即将从头全量")

#    edges = scan_sample_events()
    # 2. 写在 main() 里
    edges = scan_sample_events()
    if edges.empty or 'from' not in edges.columns or 'to' not in edges.columns:
        print("⚠️  xdog_edges.csv 为空或缺列，程序终止。")
        return
    all_addrs = set(edges['from']) | set(edges['to'])
    all_addrs = set(edges['from']) | set(edges['to'])
    bal_map, nonce_map = batch_balance_and_nonce(all_addrs)
    report = detect_dust_clusters(edges, bal_map, nonce_map)
    print(f"检出粉尘堆 {len(report)} 组")
    real_holding_rank(edges, bal_map, report)
    print("\nQuickDemo 完成！输出文件：\n"
          "  xdog_edges.csv        – 样本转账边（实时追加）\n"
          "  dust_candidate.jsonl  – 候选地址（实时追加）\n"
          "  dust_report.jsonl     – 粉尘堆（实时追加）\n"
          "  dust_graph.edges.csv  – 图边（实时追加）\n"
          "  real_holding_top.csv  – 真实持仓")

if __name__ == "__main__":
    main()