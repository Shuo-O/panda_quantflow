from panda_backtest.main_local import Run

import os
import time

def main():
    strategy_risk_control_list = []
    # strategy_risk_control_list = [{
    #     'risk_control_code_file': './strategy/risk/order_risk.py',
    #     'risk_control_name': '本地风控',
    #     'risk_control_id': 332,
    # }]
    handle_message = {'file': 'strategy/future01.py',
    # handle_message = {'file': '/Users/peiqi/code/python/panda_workflow/src/panda_backtest/strategy/ase.py',
                      'run_params': 'no_opz',
                      'start_capital': 10000000,
                      'start_date': '20241122',
                      'end_date': '20241201',
                      'standard_symbol': '000001.SH',
                      'commission_rate': 1,
                      'slippage': 0,
                      'frequency': '1d',
                      'matching_type': 1,       # 0：bar收盘，1：bar开盘
                      'run_type': 1,
                      'back_test_id': "680a0b2cdc41ea0d5d008bce",
                      'mock_id': '100',
                      'account_id': '15032863',
                      # 'future_account_id': '5588',
                      # 'fund_account_id': '2233',
                      'account_type': 0,  # 0:股票，1：期货，2：股票、期货，3：基金，4：股票基金，5：期货基金，6：所有
                      'margin_rate': 1,
                      'start_future_capital': 10000000,
                      'start_fund_capital': 1000000,
                      # 'date_type': 0,
                      # 'strategy_risk_control_list': strategy_risk_control_list
                      }

    Run.start(handle_message)

if __name__ == '__main__':
    print('进程id' + str(os.getpid()))
    main()
    # while True:
    #     time.sleep(10)
