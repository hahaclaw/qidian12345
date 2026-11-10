# -*- coding: utf-8 -*-
"""启动点12345 多空信号 —— 全市场主力合约扫描 + 企业微信群机器人推送（逻辑与文华指标一致）

注意：本文件不能命名为 signal.py —— 会遮蔽 Python 标准库 signal 模块，
导致 pandas/pyarrow 初始化时循环导入崩溃（实测 AttributeError: partially
initialized module 'pandas'）。
"""
import os, json, time, math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import akshare as ak

# ========= 信号参数 =========
PERIODS = ["15", "30", "60", "D"]      # 分钟周期可选 1/5/15/30/60；D=日线
N1, N2, N3 = 5, 10, 20
L1, L2 = 60, 120
ZH, ZP, ZF, ZQ = 0.6, 0.4, 0.8, 10

# ========= 资金管理参数 =========
def _num(name, default):
    try:
        return float((os.getenv(name) or str(default)).strip())
    except ValueError:
        return float(default)

ACCOUNT = _num("ACCOUNT", 50000)     # 账户余额（元）
RISK_PCT = _num("RISK_PCT", 2)       # 单笔风险比例（%）
RR = _num("RR", 2)                   # 盈亏比：目标价 = 入场 ± RR × 单位风险

WECOM_WEBHOOK = os.environ.get("WECOM_WEBHOOK", "")   # 企业微信群机器人完整 Webhook 地址
STATE_FILE = "state.json"
EXCLUDE = set()                        # 不想扫的品种代码，例如 {"WR0", "BB0"}
STALE_DAYS = 10                        # 最后一根K线超过这么多天视为停牌，跳过
WORKERS = 8                            # 并发取数线程数
BJ = timezone(timedelta(hours=8))

# 活跃度门槛：只扫主力品种。低于任一门槛视为不活跃（成交清淡、信号无意义），整品种跳过。
# 依据：主力连续合约 XX0，5 日成交量中位数 / 最新持仓量。
try:
    MIN_VOL = int((os.getenv("MIN_VOL") or "10000").strip())
except ValueError:
    MIN_VOL = 10000
try:
    MIN_OI = int((os.getenv("MIN_OI") or "10000").strip())
except ValueError:
    MIN_OI = 10000

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

# 新浪交易所英文名 → 中文
EX_NAME = {"shfe": "上期所", "dce": "大商所", "czce": "郑商所",
           "ine": "能源中心", "gfex": "广期所", "cffex": "中金所"}

# 品种 → (交易单位/合约乘数, 最小变动价位)
# 用于仓位计算：每手风险 = |入场价 − 止损价| × 乘数
SPEC = {
    # 上期所 / 能源中心
    "CU": (5, 10), "AL": (5, 5), "ZN": (5, 5), "PB": (5, 5), "NI": (1, 10), "SN": (1, 10),
    "AU": (1000, 0.02), "AG": (15, 1), "RB": (10, 1), "WR": (10, 1), "HC": (10, 1),
    "SS": (5, 5), "FU": (10, 1), "BU": (10, 1), "RU": (10, 5), "SP": (10, 2),
    "AO": (20, 1), "BR": (5, 5), "SC": (1000, 0.1), "NR": (10, 5), "LU": (10, 1),
    "BC": (5, 10), "EC": (50, 0.1),
    # 大商所
    "A": (10, 1), "B": (10, 1), "M": (10, 1), "Y": (10, 2), "P": (10, 2), "C": (10, 1),
    "CS": (10, 1), "JD": (10, 1), "LH": (16, 5), "L": (5, 1), "V": (5, 1), "PP": (5, 1),
    "EG": (10, 1), "EB": (5, 1), "PG": (20, 1), "J": (100, 0.5), "JM": (60, 0.5),
    "I": (100, 0.5), "FB": (10, 0.5), "BB": (500, 0.05), "RR": (10, 1), "LG": (90, 0.5),
    # 郑商所
    "SR": (10, 1), "CF": (5, 5), "CY": (5, 5), "ZC": (100, 0.2), "TA": (5, 2), "MA": (10, 1),
    "FG": (20, 1), "SA": (20, 1), "UR": (20, 1), "OI": (10, 1), "RM": (10, 1), "RS": (10, 1),
    "AP": (10, 1), "CJ": (5, 5), "PK": (5, 2), "PF": (5, 2), "SF": (5, 2), "SM": (5, 2),
    "SH": (30, 1), "PX": (5, 2), "PR": (15, 2), "WH": (20, 1), "PM": (50, 1), "RI": (20, 1),
    "LR": (20, 1), "JR": (20, 1),
    # 广期所
    "SI": (5, 5), "LC": (1, 50), "PS": (3, 5),
    # 中金所
    "IF": (300, 0.2), "IH": (300, 0.2), "IC": (200, 0.2), "IM": (200, 0.2),
    "T": (10000, 0.005), "TF": (10000, 0.005), "TS": (20000, 0.002), "TL": (10000, 0.01),
}

# 文华财经/手机交易软件里的品种名（与新浪命名不一致的才需要列）
# 文华搜索支持字母品种代码，主连合约在文华里叫「XX主连」
WH_NAME = {
    "AU": "沪金", "AG": "沪银", "CU": "沪铜", "AL": "沪铝", "ZN": "沪锌",
    "PB": "沪铅", "NI": "沪镍", "SN": "沪锡",
    "RB": "螺纹", "HC": "热卷", "RU": "橡胶", "FU": "燃油",
    "SC": "原油", "BR": "合成橡胶", "OP": "胶版纸", "EC": "集运欧线",
    "PP": "聚丙烯", "PG": "液化气", "BZ": "纯苯",
    "IF": "沪深300", "IH": "上证50", "IC": "中证500", "IM": "中证1000",
    "T": "十年国债", "TF": "五年国债", "TS": "两年国债", "TL": "三十年国债",
}


def label(sym: str, name: str) -> str:
    """消息里的品种标识，采用文华财经的叫法：品种名 + 「主连」+ 新浪代码。
    文华把这类序列叫「主连」，新浪叫「连续」，统一用文华叫法方便对照。"""
    code = sym[:-1] if sym.endswith("0") else sym
    return f"{WH_NAME.get(code, name)}主连({sym})"


_MAIN_CACHE = {}
_MARK_NAMES = None


def _mark_names():
    """新浪「品种命名表」里的中文名列表，用于实时接口取名。"""
    global _MARK_NAMES
    if _MARK_NAMES is None:
        try:
            _MARK_NAMES = [str(x) for x in _retry(ak.futures_symbol_mark)["symbol"].tolist()]
        except Exception:
            _MARK_NAMES = []
    return _MARK_NAMES


def _from_realtime(keys):
    """在实时接口里按持仓量找主力合约（排除 XX0 连续合约）。"""
    seen = set()
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            df = ak.futures_zh_realtime(symbol=key)
            if df is None or df.empty:
                continue
            df = df[~df["symbol"].astype(str).str.endswith("0")]
            if df.empty:
                continue
            row = df.sort_values("position", ascending=False).iloc[0]
            code = str(row["symbol"]).strip()
            digits = "".join(ch for ch in code if ch.isdigit())
            if digits:
                return (code, digits, float(row["position"]))
        except Exception:
            continue
    return None


def _from_months(letters: str):
    """兜底：按品种代码 + 未来12个月代码逐个试，取持仓量最大的。"""
    now = now_bj()
    codes = []
    for i in range(12):
        yy = now.year + (now.month - 1 + i) // 12
        mm = (now.month - 1 + i) % 12 + 1
        codes.append((f"{yy % 100:02d}{mm:02d}"))
    best = None

    def probe(mm):
        try:
            d = ak.futures_zh_daily_sina(symbol=f"{letters}{mm}")
            if d is None or d.empty:
                return None
            return (f"{letters}{mm}", mm, float(d["hold"].iloc[-1]))
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        for r in pool.map(probe, codes):
            if r and (best is None or r[2] > best[2]):
                best = r
    return best


def main_contract(sym: str, sina_name: str):
    """查询该品种当前主力合约，返回 (代码, 月份数字, 持仓量) 或 None。
    例如 黄金 -> ('AU2612', '2612', 149043)。只对产生信号的品种调用，开销小。"""
    if sym in _MAIN_CACHE:
        return _MAIN_CACHE[sym]
    code0 = sym[:-1] if sym.endswith("0") else sym
    wh = WH_NAME.get(code0)

    # 候选名：新浪显示名、文华名，以及品种表里以它们开头的名字（如「上证50指数期货」）
    keys = [sina_name, wh, sina_name.replace("指数", "")]
    for mk in _mark_names():
        for base in (sina_name, wh):
            if base and mk.startswith(base) and mk not in keys:
                keys.append(mk)
                break

    res = _from_realtime(keys)
    if res is None:
        res = _from_months(code0)
    _MAIN_CACHE[sym] = res
    return res


def trade_contract(sym: str, name: str):
    """返回可交易主力合约的展示串，如 '氧化铝2701(AO2701)[M]'；识别失败返回 None。"""
    mc = main_contract(sym, name)
    if not mc:
        return None
    code, digits, _ = mc
    letters = code[: len(code) - len(digits)] if digits else code
    wh = WH_NAME.get(letters, name)
    sep = " " if wh[-1:].isdigit() else ""      # 上证50 + 2612 避免粘成 上证502612
    return f"{wh}{sep}{digits}({code})[M]"


def head_and_ref(sym: str, name: str):
    """消息行开头的标识：优先用可交易主力合约，主连代码作参考。
    返回 (加粗标识, 参考串)。识别不出主力时明确标注，避免误导。"""
    tc = trade_contract(sym, name)
    if tc:
        return f"**{tc}**", f"主连{sym}"
    return f"**{label(sym, name)}**", "主力未识别"


def now_bj():
    return datetime.now(BJ)


def _clean_name(name: str) -> str:
    """去掉新浪名称里的“连续”“期货”等后缀，只留品种名"""
    return str(name).replace("连续", "").replace("期货", "").strip()


def _retry(fn, tries=3, **kw):
    """上游偶发返回 HTML 错误页，重试几次。"""
    for i in range(tries):
        try:
            return fn(**kw)
        except Exception as e:
            if i == tries - 1:
                raise
            print(f"  取数重试 {i+1}/{tries-1}: {type(e).__name__}")
            time.sleep(2 * (i + 1))


def get_symbols():
    """返回 [(代码, 名称, 交易所)]，优先从新浪动态获取全部主力连续合约"""
    try:
        df = _retry(ak.futures_display_main_sina)
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
    cols = ["datetime", "open", "high", "low", "close"]
    cols += [c for c in ("volume", "hold") if c in df.columns]   # 活跃度判断要用
    df = df[cols].astype({c: float for c in cols if c != "datetime"})
    return df.reset_index(drop=True)


def activity(df: pd.DataFrame):
    """返回 (5日成交量中位数, 最新持仓量)。缺列时返回 None。"""
    try:
        vol = float(df["volume"].astype(float).tail(5).median())
        oi = float(df["hold"].astype(float).iloc[-1])
        return vol, oi
    except Exception:
        return None


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
    # 仓位计算要用：三根短均线的上/下沿，以及近 ZQ 根的最高/最低
    df["MAXS"], df["MINS"] = maxs, mins
    df["LLV"], df["HHV"] = l.rolling(ZQ).min(), h.rolling(ZQ).max()
    return df


def round_tick(x, tick):
    """按最小变动价位取整。"""
    return round(round(x / tick) * tick, 6)


def position_plan(code, side, row):
    """仓位计算文本（markdown 引用块）。
    手数 = ⌊账户余额 × 风险比例 ÷ (|入场价 − 止损价| × 交易单位)⌋"""
    unit, tick = SPEC.get(code, (10, 1))
    entry = float(row["close"])
    if side == "多":
        stop = round_tick(min(row["LLV"], row["MINS"]) - tick, tick)
        stop_src = f"min(近{ZQ}根最低 {row['LLV']:g}, 三均线最低 {row['MINS']:.2f}) − 1跳"
        target = round_tick(entry + RR * (entry - stop), tick)
    else:
        stop = round_tick(max(row["HHV"], row["MAXS"]) + tick, tick)
        stop_src = f"max(近{ZQ}根最高 {row['HHV']:g}, 三均线最高 {row['MAXS']:.2f}) + 1跳"
        target = round_tick(entry - RR * (stop - entry), tick)

    allow = ACCOUNT * RISK_PCT / 100
    diff = abs(entry - stop)
    per_lot = diff * unit
    lots = int(math.floor(allow / per_lot)) if per_lot > 0 else 0
    total = lots * per_lot
    lines = [
        f"入场价：{entry:g}（信号K线收盘）",
        f"止损价：{stop:g}  ← {stop_src}",
        f"目标价：{target:g}（{RR:g}R 出场参考）",
        f"允许亏损 = {ACCOUNT:g} × {RISK_PCT:g}% = {allow:g} 元",
        f"每手风险 = |{entry:g} − {stop:g}| × {unit} = {per_lot:g} 元",
        (f"手数 = ⌊{allow:g} ÷ {per_lot:g}⌋ = **{lots} 手**" if lots > 0
         else f"手数 = ⌊{allow:g} ÷ {per_lot:g}⌋ = 0 手 ⚠ 资金不足，建议放弃或缩小止损"),
        f"实际总风险 = {lots} × {per_lot:g} = {total:g} 元"
        f"（{(total / ACCOUNT * 100 if ACCOUNT else 0):.2f}%）",
    ]
    return "\n".join(">" + s for s in lines)


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

    # 活跃度过滤：只保留主力品种，成交清淡的整品种跳过（含其所有周期）
    inactive, active = [], []
    for sym, name, ex in symbols:
        d = data.get((sym, "D"))
        if d is None or d.empty:
            continue
        act = activity(d)
        if act is None:
            active.append((sym, name, ex))
            continue
        vol, oi = act
        if vol < MIN_VOL or oi < MIN_OI:
            inactive.append((sym, name, vol, oi))
        else:
            active.append((sym, name, ex))
    if inactive:
        print(f"不活跃跳过 {len(inactive)} 个（门槛 成交量{MIN_VOL} / 持仓{MIN_OI}）：")
        for sym, name, vol, oi in sorted(inactive, key=lambda x: x[2]):
            print(f"  {sym} {name}: 5日量 {vol:.0f} 持仓 {oi:.0f}")
    print(f"实际计算 {len(active)} 个品种")

    duo_msgs, kong_msgs = [], []
    stale = 0
    for sym, name, ex in active:
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
            head, ref = head_and_ref(sym, name)      # 可交易主力合约在前，主连代码作参考
            code0 = sym[:-1] if sym.endswith("0") else sym
            if bool(row["DUO"]):
                side = "多"
                title = f"### <font color=\"warning\">🔴 ↑ 做多</font> {head} {EX_NAME.get(ex, ex)} {ptxt}"
            elif bool(row["KONG"]):
                side = "空"
                title = f"### <font color=\"info\">🟢 ↓ 做空</font> {head} {EX_NAME.get(ex, ex)} {ptxt}"
            else:
                continue
            block = (f"{title}\n"
                     f">K线：{bar_id} · {ref} · 收盘 {row['close']}\n"
                     f"{position_plan(code0.upper(), side, row)}\n")
            (duo_msgs if side == "多" else kong_msgs).append(block)
            state[key] = bar_id

    print(f"停牌跳过 {stale} 个")
    if duo_msgs or kong_msgs:
        body = (f"## 启动点12345 信号  {now:%m-%d %H:%M}  多{len(duo_msgs)} / 空{len(kong_msgs)}\n"
                f"账户 {ACCOUNT:g} 元 · 风险 {RISK_PCT:g}%/单 · 盈亏比 {RR:g}\n\n"
                + "\n".join(duo_msgs + kong_msgs))
        print("----- 推送内容 -----\n" + body + "--------------------")   # 日志留档，便于事后核对
        if not wecom_push(body):
            raise SystemExit("推送失败：请检查 WECOM_WEBHOOK")
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("done, 多:", len(duo_msgs), "空:", len(kong_msgs), f"总耗时 {time.time()-t0:.0f} 秒")


if __name__ == "__main__":
    main()
