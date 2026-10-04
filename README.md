# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力；并内置**仓干配协同流程**：国际班列、高标仓、干线车队在同一套谱系、预约与凭证之上协同作业。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```

## 仓干配协同

`LogisticsService`（`src/civicflow/logistics.py`）在资源预约、事件收件箱、凭证与历史能力之上提供：

- **订单与谱系**：箱货(box)、托盘(pallet)、批次(batch) 可拆分合并，每次变动写入不可变谱系
  `logistics_lineage`；`verify_conservation()` 按订单证明 登记量 = 现存(活动+交铁路) + 累计损耗。
- **多维预约**：预约同时校验车型、温区、货类、平台能力（卸货/质检/库位/叉车/装箱/铁路接口）、
  库位温区匹配、资源容量与铁路截关窗口；改配时旧预约显式 `superseded`，容量立即释放。
- **逐段交接**：到场、卸货、质检、上架、拣选、装箱、交铁路七段逐单元确认数量；
  收到+拒收+短少必须等于应收，破损不得大于收到；破损/拒收/短少自动开差异单。
- **签收不回滚**：列车改期、车辆迟到、拒收、破损、转仓只重排尚未完成且真正受影响的环节；
  已完成环节（含交接凭证）随链迁移，责任不可撤销。
- **消息纪律**：相同来源序号+相同内容按重复回执处理（不再次占用资源）；标识相同但内容冲突的
  消息进入 `inbox_conflicts` 隔离，核对前不参与业务，核对后才可采纳或保留。
- **职责隔离**：客户 scope 只能查自己订单（`read:own_orders` + `customer:<id>`）；承运方只能
  领取分配给自己的任务；差异提交人不能复核自己的差异（`assert_distinct`）。
- **运营接口**：`track` 直接回答当前去向、下一责任人、预计出库区间；`resource_gaps` 报告
  满载资源、临期未指派任务与无链订单；`earliest_slot` 给出最早补救窗口。
- **重启续跑**：预约到期、滞留、截关守望持久化在 `logistics_watches`，进程重启后 `sweep`
  继续处理；滞留告警按退避重新武装；定时任务租约过期后可被重新领取。

运行仓干配演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/logistics-demo.sqlite3 demo-logistics
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/logistics-demo.sqlite3 track <order_id>
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/logistics-demo.sqlite3 sweep
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/logistics-demo.sqlite3 gaps
```
