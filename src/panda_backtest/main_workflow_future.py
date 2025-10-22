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
import logging
import os
import time
import json
from bson import ObjectId
def start(code:str,start_future_capital:int,future_account_id:str,start_date:str,end_date:str,commission_rate:int,margin_rate:int,frequency:str,df_factor:pd.DataFrame):
    strategy_risk_control_list = []
    back_test_id = str(ObjectId())

    handle_message = {'code': code,
    # handle_message = {'file': '/Users/peiqi/code/python/panda_workflow/src/panda_backtest/strategy/ase.py',
                      'run_params': 'no_opz',
                      'start_capital': 10000000,
                      'start_date': start_date,
                      'end_date': end_date,
                      'standard_symbol': '000001.SH',
                      'commission_rate': commission_rate,
                      'slippage': 0,
                      'frequency': frequency,
                      'matching_type': 1,       # 0：bar收盘，1：bar开盘
                      'run_type': 1,
                      'back_test_id': back_test_id,
                      'mock_id': '100',
                      'future_account_id': future_account_id,
                      'account_type': 1,  # 0:股票，1：期货，2：股票、期货，3：基金，4：股票基金，5：期货基金，6：所有
                      'margin_rate': margin_rate,
                      'start_future_capital': start_future_capital,
                      'start_fund_capital': 1000000,
                      }
    # 系统核心上下文 创建q
    strategy_context = StrategyContext()
    if not df_factor.empty:
        strategy_context.init_factor_params(df_factor)
    param_dict = dict()
    run_params = handle_message['run_params']
    if run_params is None or run_params == 'no_run_params' or run_params == '' or run_params =='no_opz':
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

    DevInit.init_log_env('panda')
    DevInit.init_remote_sr_log(back_test_id, handle_message['run_params'], strategy_context)

    # 全局动态字典初始化
    global_args = {}

    run_type = 6
    global_args = FileStrategyLoader(
        code, False).load(global_args)
    handle_message['strategy_id'] = 1
    extension_module = import_module("panda_backtest.extensions.trade_reverse_future")
    extension = extension_module.load_extension()
    extension.create(_context)
    Strategy(global_args, _context.event_bus)

    # SRLogger.info(str(handle_message))
    # 项目启动
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
    fworom panda_backtest.system.panda_log import SRLogger
    from panda_backtest.api.api import *
    import pandas as pd
    import numpy as np
    import copy
    import datetime
    import re
    import pickle
    import sys

    def initialize(context):
        SRLogger.info("策略初始化")
        context.account = '5588'
        context.trading_code="AG2509.SHFE"

    def before_trading(context):
        SRLogger.info("交易前")

    def handle_data(context, bar_dict):
        if (int(context.now) % 2) == 0:
            buy_open(account_id=context.account,id_or_ins=context.trading_code,amount=1)
        else:
            sell_open(account_id=context.account,id_or_ins=context.trading_code,amount=-1)

    def after_trading(context):
        SRLogger.info("交易后")
    '''.strip()
    print('进程id' + str(os.getpid()))
    start(code=strategy_code_default, start_future_capital=100000, future_account_id='000001',start_date='20250101',end_date='20250201')


