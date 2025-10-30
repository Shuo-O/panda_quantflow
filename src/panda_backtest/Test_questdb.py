from panda_backtest.main_local import Run

import os
import time
import uuid


def main():
    handle_message = {
        'file': 'strategy/future01.py',
        'run_params': 'no_opz',
        'start_capital': 10000000,
        'start_future_capital': 10000000,
        'start_fund_capital': 1000000,
        'start_date': '20241122',
        'end_date': '20241201',
        'standard_symbol': '000001.SH',
        'commission_rate': 1,
        'slippage': 0,
        'frequency': '1d',
        'matching_type': 1,
        'run_type': 1,
        'back_test_id': uuid.uuid4().hex[:24],
        'mock_id': '100',
        'account_id': '15032863',
        'account_type': 0,
        'margin_rate': 1,
        'data_source': 'questdb',
    }

    Run.start(handle_message)


if __name__ == '__main__':
    print('QuestDB回测进程id' + str(os.getpid()))
    main()
