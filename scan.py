# -*- coding: utf-8 -*-
"""启动点12345 多空信号 —— 云端扫描 + 企业微信推送（逻辑与文华指标一致）

注意：本文件不能命名为 signal.py —— 会遮蔽 Python 标准库 signal 模块，
导致 pandas/pyarrow 初始化时循环导入崩溃。
"""
import os, json, time
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import akshare as ak

# ========= 可调参数 =========
SYMBOLS = ["RB0", "I0", "MA0", "TA0", "AU0", "CU0", "M0", "SA0"]   # 新浪主力连续代码
PERIODS = ["15", "30", "60", "D"]        # 分钟周期: 1/5/15/30/60 ；日线: D
N1, N2, N3 = 5, 10, 20
L1, L2 = 60, 120
ZH, ZP, ZF, ZQ = 0.6, 0.4, 0.8, 10
WECOM_WEBHOOK = os.environ.get("WECOM_WEBHOOK", "")   # 企业微信机器人，放在 GitHub Secrets
STATE_FILE = "state.json"
BJ = timezone(timedelta(hours=8))
WECOM_LIMIT = 2000   # 企业微信 text 消息约 2048 字节，留余量


def now_bj():
    return datetime.now(BJ)


def _chunks(text, limit):
    """按行切分，保证每段编码后不超过 limit 字节。"""
    out, cur = [], ""
    for line in text.split("\n"):
        candidate = f"{cur}\n{line}" if cur else line
        if len(candidate.encode()) <= limit:
            cur = candidate
            continue
        if cur:
            out.append(cur)
        while len(line.encode()) > limit:
            cut = limit // 3
            out.append(line[:cut])
            line = line[cut:]
        cur = line
    if cur:
        out.append(cur)
    return out


def fetch(symbol: str, period: str) -> pd.DataFrame:
    if period == "D":
        df = ak.futures_zh_daily_sina(symbol=symbol)
        df = df.rename(columns={"date": "datetime"})
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
    # 1 粘合（近3根任一）
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


def push(title: str, body: str) -> bool:
    """推送到企业微信群机器人。返回是否全部成功。"""
    if not WECOM_WEBHOOK:
        print("未配置 WECOM_WEBHOOK，仅打印：\n", title, "\n", body)
        return False
    ok = True
    for part in _chunks(f"{title}\n{body}", WECOM_LIMIT):
        try:
            r = requests.post(WECOM_WEBHOOK, timeout=20,
                              json={"msgtype": "text", "text": {"content": part}})
            j = r.json()
            if j.get("errcode") == 0:
                print(f"企业微信: ok ({len(part.encode())} bytes)")
            else:
                print(f"企业微信失败: errcode={j.get('errcode')} {j.get('errmsg')}")
                ok = False
        except Exception as e:
            print(f"企业微信异常: {e}")
            ok = False
    return ok


def main():
    try:
        state = json.load(open(STATE_FILE, encoding="utf-8"))
    except Exception:
        state = {}
    msgs = []
    now = now_bj()
    for sym in SYMBOLS:
        for per in PERIODS:
            try:
                df = calc(fetch(sym, per))
            except Exception as e:
                print(f"{sym}-{per} 取数失败: {e}")
                continue
            if df.empty:
                continue
            # 日线：盘中取到的最后一根是当天未收盘的K线，此时判断会产出假信号，跳过
            if per == "D":
                last_day = pd.to_datetime(df["datetime"].iloc[-1]).date()
                if last_day == now.date() and now.hour < 15:
                    continue
            # 分钟线最后一根可能未走完，取倒数第二根；日线取最后一根（已收盘）
            row = df.iloc[-1] if per == "D" else df.iloc[-2]
            key = f"{sym}-{per}"
            bar_id = str(row["datetime"])
            if state.get(key) == bar_id:
                continue
            if bool(row["DUO"]) or bool(row["KONG"]):
                side = "🔴 做多 ↑" if row["DUO"] else "🟢 做空 ↓"
                # 企业微信 text 消息不渲染 Markdown，不要用 ** 加粗
                msgs.append(f"{sym} {per}{'日线' if per=='D' else '分钟'}  {side}  "
                            f"收盘 {row['close']}  K线 {bar_id}")
                state[key] = bar_id
            time.sleep(0.5)

    if msgs:
        if not push("启动点12345 信号（%d条）" % len(msgs),
                    "\n".join(msgs) + f"\n\n扫描时间 {now:%Y-%m-%d %H:%M}"):
            raise SystemExit("推送失败：请检查 WECOM_WEBHOOK")
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("done, signals:", len(msgs))


if __name__ == "__main__":
    main()
