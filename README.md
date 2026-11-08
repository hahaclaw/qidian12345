# 启动点12345 多空信号

GitHub Actions 定时扫描国内商品期货主力连续合约，出现启动点信号时推送到企业微信群。

## 原理

```
GitHub Actions 定时(cron) → akshare 拉新浪期货K线 → 判断最新已收盘K线
→ 有信号则推送到企业微信 → 已推送记录写入 state.json 提交回仓库，防止重复
```

## 文件

| 文件 | 作用 |
|---|---|
| `scan.py` | 扫描主程序（文件名不能改成 `signal.py`，会与 Python 标准库冲突） |
| `requirements.txt` | Python 依赖 |
| `state.json` | 去重状态，每次运行后自动提交 |
| `.github/workflows/signal.yml` | 定时任务 |

## 配置

在 Settings → Secrets and variables → Actions 添加：

- `WECOM_WEBHOOK` —— 企业微信群机器人的 Webhook 地址

获取方式：企业微信群 → 群机器人 → 添加机器人 → 复制 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...`

## 品种与周期

`scan.py` 顶部的可调参数：

```python
SYMBOLS = ["RB0", "I0", "MA0", "TA0", "AU0", "CU0", "M0", "SA0"]
PERIODS = ["15", "30", "60", "D"]
```

## 已知限制

- **定时频率高**：工作日 52 次/天，每月约 1300 次运行。仓库设为公开时 Actions 分钟数免费不限；若是私有仓库，2000 分钟/月的免费额度约十天就会耗尽。
- **日线只在收盘后判断**：盘中取到的日线是未完成K线，代码会跳过，避免推送假信号。
- **分钟线只看倒数第二根**：没有回看窗口，若某次定时触发被 GitHub 延迟或跳过，那一根K线的信号可能漏报。
- GitHub 定时任务通常延迟 5～30 分钟，属于正常现象。
