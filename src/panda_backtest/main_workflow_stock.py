import pandas as pd
from panda_backtest.main_local import Run
from panda_backtest.data.context.strategy_context import StrategyContext
from panda_backtest.config.dev_init import DevInit
from importlib import import_module
from panda_backtest.backtest_common.system.context.core_context import CoreContext
from panda_backtest.backtest_common.system.event.engine import Engine
from panda_backtest.backtest_common.system.compile.strategy import Strategy
from panda_backtest.backtest_common.system.compile.strategy_utils import FileStrategyLoader
import traceback
from panda_backtest.system.panda_log import SRLogger

import json
from bson import ObjectId
def get_backtest_id():
    return str(ObjectId())

def start(back_test_id:str,code:str,start_date:str,end_date:str, start_capital: int, standard_symbol: str,
          commission_rate: int, account_id: str, df_factor: pd.DataFrame,frequency:str):
    symbol_map = {
        "上证指数": "000001.SH",
        "沪深300": "000300.SH",
        "中证500": "000500.SH",
        "中证1000": "001000.SH"
    }
    standard_symbol_pro = symbol_map.get(standard_symbol)
    handle_message = {'code': code,
                      'file':'/Users/peiqi/code/python/panda_workflow/src/panda_backtest/strategy/stock01.py',
                      'run_params': 'no_opz',
                      'start_capital': start_capital,
                      'start_date': start_date,
                      'end_date': end_date,
                      'standard_symbol': standard_symbol_pro,
                      'commission_rate': commission_rate,
                      'slippage': 0,
                      'frequency': frequency,
                      'matching_type': 1,  # 0：bar收盘，1：bar开盘
                      'run_type': 1,
                      'back_test_id': back_test_id,
                      'mock_id': '100',
                      'stock_account': account_id,
                      'future_account_id': '5588',
                      'fund_account_id': '2233',
                      'account_type': 0,  # 0:股票，1：期货，2：股票、期货，3：基金，4：股票基金，5：期货基金，6：所有
                      'margin_rate': 1,
                      'start_future_capital': 10000000,
                      'start_fund_capital': 1000000,
                      'date_type': 0
                      }
    # LogFactory.init_logger() - 已替换为统一日志配置

    # 系统核心上下文 创建q
    strategy_context = StrategyContext()
    if not df_factor.empty:
        strategy_context.init_factor_params(df_factor)
    param_dict = dict()
    run_params = handle_message['run_params']
    if run_params is None or run_params == 'no_run_params' or run_params == '' or run_params == 'no_opz':
        pass
    else:
        print('运行时参数', handle_message['run_params'])
        param_dict = dict()
        run_params_list = json.loads(run_params)
        for run_params_item in run_params_list:
            # if run_params_item[0] == 0:
            #     param_dict[run_params_item[1]] = float(run_params_item[2])
            # else:
            #     param_dict[run_params_item[1]] = run_params_item[2]
            if run_params_item[0] == 0:
                param_dict[run_params_item[1]] = float(run_params_item[2])
            elif run_params_item[0] == 1:
                param_dict[run_params_item[1]] = run_params_item[2]

    strategy_context.init_opz_params(param_dict)
    _context = CoreContext(strategy_context)

    back_test_id = handle_message['back_test_id']
    DevInit.init_log_env('panda')
    DevInit.init_remote_sr_log(back_test_id, handle_message['run_params'], strategy_context)

    # 全局动态字典初始化
    global_args = {}

    global_args = FileStrategyLoader(code, False).load(global_args)
    # global_args = FileStrategyLoader(handle_message['file'], True).load(global_args)
    handle_message['strategy_id'] = 1
    extension_module = import_module("panda_backtest.extensions.trade_reverse_future")
    extension = extension_module.load_extension(handle_message.get('data_source'))
    extension.create(_context)
    Strategy(global_args, _context.event_bus)

    # SRLogger.info(str(handle_message))
    # 项目启动
    try:
        Engine(_context).run(handle_message)
    except Exception as e:
        # 打印异常的堆栈信息
        print(traceback.format_exc())
        raise
    finally:
        # 无论是否有异常，确保结束日志
        SRLogger.end()
    return back_test_id
if __name__ == '__main__':
    strategy_code_default = '''
from panda_backtest.api.api import *
import pandas as pd
import numpy as np
from panda_backtest.api.stock_api import *
import panda_data
import traceback
# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递对象将会在你的算法策略的任何方法之间做传递。
def initialize(context):
    panda_data.init()
    SRLogger.info("策略初始化开始")
    SRLogger.info("adjustment_cycle:" + str(context.adjustment_cycle) )
    SRLogger.info("group_number:" + str(context.group_number))
    
    # 自定义累计交易日，用于换仓判断
    context.trade_days = 0

    context.df_factor =context.df_factor.shift(-1).reset_index()


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    # 根据调仓周期判断当天是否交易
    if context.trade_days == 0:
        context.flag = True
        # 获取目标持仓
        context.trade_df=get_target_positions(context)
    # 未到调仓周期
    elif context.trade_days % context.adjustment_cycle != 0:
        context.flag = False
    # 调仓
    elif context.trade_days % context.adjustment_cycle == 0:
        context.flag = True
        # 获取目标持仓
        df_target = get_target_positions(context)
        # 获取当前持仓
        df_current= context.stock_account_dict['8888'].positions.to_dataframe()
        context.trade_df= calculate_rebalance_table(df_target, df_current)
    SRLogger.info(f"交易前{context.trade_days}")


# 你选择的标的的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_data(context,bar_dict):
    # 下单逻辑
    stock_account = context.run_info.stock_account
    for index, row in  context.trade_df.iterrows():
        symbol = row["symbol"]
        positions = int(row["positions"])
        try:
            order_shares(stock_account, symbol, positions)
        except Exception as e:
            traceback.print_exc()
            print(f"下单失败: day={context.now},symbol={symbol}, positions={positions}, 错误信息: {e}")

# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    SRLogger.info("交易后")
    # 交易日加1
    context.trade_days += 1
    print(f"交易后: day={context.now},total_value ={context.stock_account_dict['8888'].total_value}")

def count_symbol_positions(context, df_symbol:pd.DataFrame):

    total_value =context.stock_account_dict['8888'].total_value
    # 通过股票列表，获取pre_close
    df_symbol=stock_api_pre_close(df_symbol,context.now)
    avg_value_per_stock = total_value / 100 / len(df_symbol)
    df_symbol["positions"] = np.floor(avg_value_per_stock / (df_symbol["pre_close"] * 1.1)).astype(int)
    df_symbol["positions"]=df_symbol["positions"]*100
    return df_symbol
# 获取当天买入的股票
def get_target_positions(context):
    today = context.now  # 获取回测当天日期
    # 获取第三列的列名
    third_column = context.df_factor.columns[2]
    today_df_factor = context.df_factor[context.df_factor["date"] == today].sort_values(third_column, ascending=False)
    # 获取前几行的 "symbol" 列，并转换为列表
    today_trade_symbol = pd.DataFrame(
        today_df_factor.head(len(today_df_factor) // int(context.group_number))["symbol"].tolist(),columns=["symbol"])
    trade_df = count_symbol_positions(context, today_trade_symbol)

    return trade_df

def calculate_rebalance_table(df_target: pd.DataFrame, df_current: pd.DataFrame) -> pd.DataFrame:
    # 只保留 symbol 和 positions 两列
    df_target = df_target[["symbol", "positions"]].rename(columns={"positions": "target"})
    df_current = df_current[["symbol", "positions"]].rename(columns={"positions": "current"})

    # 合并两个表，按 symbol 对齐
    df_merged = pd.merge(df_target, df_current, on="symbol", how="outer").fillna(0)

    # 计算调仓差值（目标 - 当前）
    df_merged["positions"] = (df_merged["target"] - df_merged["current"]).astype(int)

    # 只保留有换仓动作的项（非零）
    df_rebalance = df_merged[df_merged["positions"] != 0][["symbol", "positions"]]

    return df_rebalance.reset_index(drop=True)
    '''.strip()
    df = pd.read_csv(
        '/Users/peiqi/df_factor_pro1.csv',
        usecols=["date", "symbol", "factor1"],  # 只读取需要的列，节省内存
        dtype={"date": str}  # 明确指定date列为字符串类型
    )
    print(df.head())

    from pathlib import Path
    import os
    import sys
    # Add project root path to python path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    # Load .env file if exsits
    import dotenv

    dotenv.load_dotenv()

    id=start(code=strategy_code_default,start_date="20250101", frequency="1d",end_date="20250301",start_capital=10000000, standard_symbol="上证指数",commission_rate=1,account_id="8888",df_factor=df)
    print(id)
