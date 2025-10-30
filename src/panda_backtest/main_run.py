"""
          ┌─┐       ┌─┐
       ┌──┘ ┴───────┘ ┴──┐
       │                 │
       │       ───       │
       │  ─┬┘       └┬─  │
       │                 │
       │       ─┴─       │
       │                 │
       └───┐         ┌───┘
           │         │
           │         │
           │         │
           │         └──────────────┐
           │                        │
           │                        ├─┐
           │                        ┌─┘
           │                        │
           └─┐  ┐  ┌───────┬──┐  ┌──┘
             │ ─┤ ─┤       │ ─┤ ─┤
             └──┴──┘       └──┴──┘
                 神兽保佑
                 代码无BUG!
"""
import json
import time
from importlib import import_module
from panda_backtest.config.dev_init import DevInit
from common.connector.mongodb_handler import DatabaseHandler
from common.config.config import config
from panda_backtest.backtest_common.exception.error_exception import ErrorException
from panda_backtest.backtest_common.risk.risk_control_manager import RiskControlManager
from panda_backtest.backtest_common.system.context.core_context import CoreContext
from panda_backtest.backtest_common.system.event.engine import Engine
from panda_backtest.backtest_common.system.compile.strategy import Strategy
from panda_backtest.backtest_common.system.compile.strategy_utils import FileStrategyLoader
import traceback
from panda_backtest.data.context.strategy_context import StrategyContext
import os
import threading
import pandas as pd
import logging

def main_run(kwargs=None):
    print('进程id' + str(os.getpid()))
    print('线程id' + str(threading.current_thread()))
    logger = logging.getLogger(__name__)
    # logger.info('日志系统已初始化')

    # 判断是否参数调优
    strategy_context = StrategyContext()

    run_params = kwargs.get('run_params', None)
    if run_params is None or run_params == 'no_run_params' or run_params == '':
        pass
    else:
        print('运行时参数', kwargs.get('run_params', None))
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
            elif run_params_item[0] == 2:
                # 假设 run_params_item[2] 是一个 JSON 字符串
                try:
                    # 如果是 JSON 字符串，先解析它为 Python 对象
                    json_data = json.loads(run_params_item[2])  # 将 JSON 字符串转换为 Python 字典或列表
                    # 将 JSON 转换为 DataFrame
                    df = pd.json_normalize(json_data)  # 或者 pd.DataFrame(json_data)，根据实际结构使用
                    param_dict[run_params_item[1]] = df
                except Exception as e:
                    print(f"Error loading JSON DataFrame : {e}")


        strategy_context.init_opz_params(param_dict)

    back_test_id = kwargs.get('back_test_id', None)
    DevInit.init_log_env('sunrise')
    DevInit.init_remote_sr_log(back_test_id, 'no_run_params', strategy_context)
    from panda_backtest.system.panda_log import SRLogger

    # 系统核心上下文 创建q
    _context = CoreContext(strategy_context)
    # 全局动态字典初始化
    global_args = {}

    run_type = kwargs.get('run_type', None)

    if run_type is None:
        return

    # 策略文件编译、自省其中方法到全局动态字典
    global_args = FileStrategyLoader(
        kwargs.get('code', None), False).load(global_args)

    # 根据情况，加载对应扩展
    extension_module = import_module("panda_backtest.extensions.trade_reverse_future")
    extension = extension_module.load_extension(kwargs.get('data_source'))
    extension.create(_context)

    # 初始化
    Strategy(global_args, _context.event_bus)

    # # 风控相关
    # # start_time = time.time()
    # risk_control_manager = RiskControlManager(MongoClient.get_mongo_db(), _context.event_bus, strategy_context)
    # risk_control_manager.init_event()
    # risk_control_manager.load_risk_control(kwargs.get('back_test_id', None))
    # _context.set_risk_control_manager(risk_control_manager)
    # print('风控相关耗时：' + str(time.time() - start_time))

    # # 事件函数入参，还没想好放什么
    handle_message = {'strategy_id': kwargs.get('strategy_id', None),
                      'custom_tag': kwargs.get('custom_tag', None),
                      'start_capital': kwargs.get('start_capital', None),
                      'start_future_capital': kwargs.get('start_future_capital', None),
                      'start_date': kwargs.get('start_date', None),
                      'end_date': kwargs.get('end_date', None),
                      'standard_symbol': kwargs.get('standard_symbol', None),
                      'commission_rate': kwargs.get('commission_rate', None),
                      'margin_rate': kwargs.get('margin_rate', None),
                      'slippage': kwargs.get('slippage', None),
                      'future_slippage': kwargs.get('future_slippage', 0),
                      'frequency': kwargs.get('frequency', None),
                      'matching_type': kwargs.get('matching_type', None),
                      'run_type': kwargs.get('run_type', None),
                      'back_test_id': kwargs.get('back_test_id', None),
                      'mock_id': kwargs.get('mock_id', None),
                      'account_id': kwargs.get('account_id', None),
                      'risk_code': kwargs.get('risk_code', None),
                      'performanceAnalysis': kwargs.get('performanceAnalysis', None),
                      'future_account_id': kwargs.get('future_account_id', None),
                      'account_type': kwargs.get('account_type', 0),
                      'start_fund_capital': kwargs.get('start_fund_capital', None),
                      'date_type': kwargs.get('date_type', 0),
                      'rate_dict_data_str': kwargs.get('rate_dict_data_str', '')
                      }
    # SRLogger.info(str(handle_message))
    # 项目启动
    try:
        Engine(_context).run(handle_message)
        print('结束掉终止信号')
        SRLogger.end()
    finally:
        print('异常结束掉终止信号')
        SRLogger.end()
