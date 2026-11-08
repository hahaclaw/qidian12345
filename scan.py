# -*- coding: utf-8 -*-
"""启动点12345 多空信号 —— 全市场主力合约扫描 + 企业微信群机器人推送（逻辑与文华指标一致）

注意：本文件不能命名为 signal.py —— 会遮蔽 Python 标准库 signal 模块，
导致 pandas/pyarrow 初始化时循环导入崩溃（实测 AttributeError: partially
initialized module 'pandas'）。
"""
import os, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import akshare as ak

# ========= 可调参数 =========
PERIODS = ["15", "30", "60", "D"]      # 分钟周期可选 1/5/15/30/60；D=日线
N1, N2, N3 = 5, 10, 20
L1, L2 = 60, 120
ZH, ZP, ZF, ZQ = 0.6, 0.4, 0.8, 10
WECOM_WEBHOOK = os.environ.get("WECOM_WEBHOOK", "")   # 企业微信群机器人完整 Webhook 地址
STATE_FILE = "state.json"
EXCLUDE = set()                        # 不想扫的品种代码，例如 {"WR0", "BB0"}
STALE_DAYS = 10                        # 最后一根K线超过这么多天视为停牌，跳过
WORKERS = 8                            # 并发取数线程数
BJ = timezone(timedelta(hours=8))

# 新浪 futures_display_main_sina() 漏掉但能正常取数的主力品种
EXTRA = {"TL0": ("30年期国债", "cffex"), "T0": ("10年期国债", "cffex")}

# 接口失败时的回退品种表（六大交易所，新浪主力连续代码 = 品种代码+0）
FALLBACK = {
    "上期所": ["CU","AL","ZN","PB","NI","SN","AU","AG","RB","WR","HC","SS","FU","BU","RU","SP","AO","BR","EC"],
    "能源中心": ["SC","NR","LU","BC"],
    "大商所": ["A","B","M","Y","P","C","CS","JD","LH","L","V","PP","EG","EB","PG","J","JM","I","FB","BB","RR","LG"],
    "郑商所": ["SR","CF","CY","ZC","TA","MA","FG","SA","UR","OI","RM","RS","AP","CJ","PK","PF","SF","SM","SH","PX","PR","WH","PM","RI","LR","JR"],
    "广期所": ["SI","LC","PS"],
    "中金所": ["IF","IH","IC","IM","T","TF","TS","TL"],
}


def now_bj():
    return datetime.now(BJ)


def _clean_name(name: str) -> str:
    """去掉新浪名称里的“连续”“期货”等后缀，只留品种名"""
    return str(name).replace("连续", "").replace("期货", "").strip()


def get_symbols():
    """返回 [(代码, 名称, 交易所)]，优先从新浪动态获取全部主力连续合约"""
    try:
        df = ak.futures_display_main_sina()
        out = [(r["symbol"], _clean_name(r["name"]), r.get("exchange", "")) for _, r in df.iterrows()]
        if len(out) >= 40:
            have = {s[0] for s in out}
            for code, (nm, ex) in EXTRA.items():
                if code not in have:
                    out.append((code, nm, ex))
            return [s for s in out if s[0] not in EXCLUDE]
    except Exception as e:
        print("动态获取主力合约列表失败，使用内置表：", e)
    out = []
    for ex, codes in FALLBACK.items():
        out += [(c + "0", c, ex) for c in codes if c + "0" not in EXCLUDE]
    return out


def fetch(symbol: str, period: str) -> pd.DataFrame:
    if period == "D":
        df = ak.futures_zh_daily_sina(symbol=symbol).rename(columns={"date": "datetime"})
    else:
        df = ak.futures_zh_minute_sina(symbol=symbol, period=period)
    df = df[["datetime", "open", "high", "low", "close"]].astype(
        {"open": float, "high": float, "low": float, "close": float})
    return df.reset_index(drop=True)


def calc(df: pd.DataFrame) -> pd.DataFrame:
    c, h, l = df["close"], df["high"], df["low"]
    ma1, ma2, ma3 = c.rolling(N1).mean(), c.rolling(N2).mean(), c.rolling(N3).mean()
    ma4, ma5 = c.rolling(L1).mean(), c.rolling(L2).mean()
    maxs = pd.concat([ma1, ma2, ma3], axis=1).max(axis=1)
    mins = pd.concat([ma1, ma2, ma3], axis=1).min(axis=1)
    # 1 粘合
    nh = (maxs - mins) / c * 100 < ZH
    nh3 = nh | nh.shift(1, fill_value=False) | nh.shift(2, fill_value=False)
    # 3 走平
    flat = ((ma1 / ma1.shift(3) - 1).abs() * 100 < ZP) & \
           ((ma2 / ma2.shift(3) - 1).abs() * 100 < ZP) & \
           ((ma3 / ma3.shift(3) - 1).abs() * 100 < ZP)
    zpx = flat | flat.shift(1, fill_value=False)
    # 4 振幅收敛
    amp = (h - l) / c * 100
    sf = amp.rolling(ZQ).mean() <= amp.rolling(ZQ * 3).mean() * ZF
    # 5 长期均线配合 / 全部粘合
    lduo = (ma4 < c) & (ma5 < c) & (c > maxs)
    lkong = (ma4 > c) & (ma5 > c) & (c < mins)
    maxa = pd.concat([maxs, ma4, ma5], axis=1).max(axis=1)
    mina = pd.concat([mins, ma4, ma5], axis=1).min(axis=1)
    lnh = (maxa - mina) / c * 100 < ZH * 2
    # 2 突破
    upall = (c > ma1) & (c > ma2) & (c > ma3)
    dnall = (c < ma1) & (c < ma2) & (c < ma3)
    tpd = upall & ~upall.shift(1, fill_value=False)
    tpk = dnall & ~dnall.shift(1, fill_value=False)
    df = df.copy()
    df["DUO"] = nh3 & zpx & sf & tpd & (lduo | lnh)
    df["KONG"] = nh3 & zpx & sf & tpk & (lkong | lnh)
    return df


def wecom_push(text: str) -> bool:
    """企业微信群机器人 markdown 推送；超长自动分段，频率≤20条/分钟。返回是否全部成功。"""
    if not WECOM_WEBHOOK:
        print("未配置 WECOM_WEBHOOK，仅打印：\n", text)
        return False
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len((cur + line + "\n").encode("utf-8")) > 3800:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    chunks.append(cur)
    ok = True
    for i, ch in enumerate(chunks):
        try:
            r = requests.post(WECOM_WEBHOOK, json={"msgtype": "markdown",
                                                   "markdown": {"content": ch}}, timeout=15)
            j = r.json()
            if j.get("errcode") == 0:
                print(f"wecom push: ok ({len(ch.encode())} bytes)")
            else:
                print(f"wecom 失败: {j}")
                ok = False
        except Exception as e:
            print("wecom 异常:", e)
            ok = False
        if i < len(chunks) - 1:
            time.sleep(3.5)
    return ok


def fetch_all(symbols):
    """并发拉取所有 (品种, 周期) 的K线。返回 {(sym, per): DataFrame}"""
    tasks = [(sym, per) for sym, _, _ in symbols for per in PERIODS]
    data, failed = {}, []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch, sym, per): (sym, per) for sym, per in tasks}
        for fut in as_completed(futures):
            sym, per = futures[fut]
            try:
                data[(sym, per)] = fut.result()
            except Exception as e:
                print(f"{sym}-{per} 取数失败: {e}")
                failed.append(f"{sym}-{per}")
    return data, failed


def main():
    try:
        state = json.load(open(STATE_FILE, encoding="utf-8"))
    except Exception:
        state = {}
    symbols = get_symbols()
    now = now_bj()
    print(f"扫描标的 {len(symbols)} 个 × 周期 {PERIODS}，并发 {WORKERS} 线程")

    t0 = time.time()
    data, failed = fetch_all(symbols)
    print(f"取数完成：成功 {len(data)} 个，失败 {len(failed)} 个，耗时 {time.time()-t0:.0f} 秒")

    duo_msgs, kong_msgs = [], []
    stale = 0
    for sym, name, ex in symbols:
        for per in PERIODS:
            df = data.get((sym, per))
            if df is None or df.empty:
                continue
            if len(df) < L2 + 5:
                continue
            # 停牌品种：最后一根K线停在很久以前，跳过，避免推历史僵尸信号
            last_dt = pd.to_datetime(df["datetime"].iloc[-1])
            if (now.replace(tzinfo=None) - last_dt).days > STALE_DAYS:
                stale += 1
                continue
            # 日线：盘中最后一根是当天未收盘的K线，判断会产出假信号，跳过
            if per == "D" and last_dt.date() == now.date() and now.hour < 15:
                continue
            df = calc(df)
            # 分钟线最后一根可能未走完取倒数第二根；日线取最后一根
            row = df.iloc[-1] if per == "D" else df.iloc[-2]
            key, bar_id = f"{sym}-{per}", str(row["datetime"])
            if state.get(key) == bar_id:
                continue
            ptxt = "日线" if per == "D" else f"{per}分钟"
            if bool(row["DUO"]):
                duo_msgs.append(f"> <font color=\"warning\">↑ 做多</font> **{name}({sym})** {ex} {ptxt}  收盘 {row['close']}  K线 {bar_id}")
                state[key] = bar_id
            elif bool(row["KONG"]):
                kong_msgs.append(f"> <font color=\"info\">↓ 做空</font> **{name}({sym})** {ex} {ptxt}  收盘 {row['close']}  K线 {bar_id}")
                state[key] = bar_id

    print(f"停牌跳过 {stale} 个")
    if duo_msgs or kong_msgs:
        body = f"## 启动点12345 信号  {now:%m-%d %H:%M}\n"
        if duo_msgs:
            body += f"**🔴 做多 {len(duo_msgs)} 个**\n" + "\n".join(duo_msgs) + "\n"
        if kong_msgs:
            body += f"**🟢 做空 {len(kong_msgs)} 个**\n" + "\n".join(kong_msgs) + "\n"
        if not wecom_push(body):
            raise SystemExit("推送失败：请检查 WECOM_WEBHOOK")
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("done, 多:", len(duo_msgs), "空:", len(kong_msgs), f"总耗时 {time.time()-t0:.0f} 秒")


if __name__ == "__main__":
    main()
