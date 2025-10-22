import datetime
import time
import traceback
from panda_backtest.backtest_common.system.context.core_context import CoreContext
from panda_backtest.backtest_common.data.quotation.quotation_data import QuotationData
from panda_backtest.backtest_common.system.event.event import ConstantEvent, Event
from panda_backtest.util.log.remote_log_factory import RemoteLogFactory
from panda_trading.trading.sub_pub import get_trade_signal_bus
from utils.data.data_util import DateUtil
from utils.log.log_factory import LogFactory
from utils.time.time_util import TimeUtil

class TradeTimeManager(object):
    def __init__(self, quotation_mongo_db):
        self.quotation_mongo_db = quotation_mongo_db
        self.context = CoreContext.get_instance()
        self.logger = LogFactory.get_logger()
        self.sub_pub = get_trade_signal_bus()
        self.trade_date = None
        if TimeUtil.in_time_range('200000-235959'):
            self.trade_date = DateUtil.get_next_trade_date(self.now)
        elif TimeUtil.in_time_range('000000-023000'):
            self.trade_date = DateUtil.get_next_trade_date(self.now, operate='$gte')
        else:
            self.trade_date = DateUtil.get_next_trade_date(self.now, operate='$gte')

    @property
    def now(self):
        return time.strftime("%Y%m%d")

    @property
    def hms(self):
        return time.strftime("%H%M%S")

    @property
    def trade_time(self):
        return datetime.datetime.now()

    def start_trade_time_play(self):
        strategy_context = self.context.strategy_context
        run_info = strategy_context.run_info

        event_bus = self.context.event_bus

        if strategy_context.enable_risk_control:
            event = Event(
                ConstantEvent.RISK_CONTROL_INIT,
                context=strategy_context)
            event_bus = self.context.event_bus
            event_bus.publish_event(event)

        event = Event(
            ConstantEvent.STRATEGY_INIT,
            context=strategy_context)
        event_bus.publish_event(event)

        event = Event(
            ConstantEvent.SYSTEM_RESTORE_STRATEGY)
        event_bus.publish_event(event)

        self.logger.info("交易信号通道: %s", type(self.sub_pub).__name__)
        self.sub_pub.init_sub_trade_signal(run_info.run_id, run_info.account_type, self.handle_trade_sub_mes)
        self.sub_pub.init_qry_account(self.qry_account_info)
        self.sub_pub.init_check_thread()
    # 接收交易信息后，开始执行回调
    def handle_trade_sub_mes(self, ch, method, properties, body):
        try:
            event_bus = self.context.event_bus
            strategy_context = self.context.strategy_context
            self.logger.info('TradeTimeManager 收到交易消息===》' + str(body))
            if body == '0':
                self.logger.info('=======================开始交易=======================》')
                # 防止handle_data任务时间过长，堆积任务
                # if (int(time.time()) - properties.timestamp) > 20:
                properties = int(int(properties) / 1000)
                if (int(time.time()) - properties) > 20:        # redis方式
                    self.logger.info('任务过时，当前时间戳:%s，任务时间戳：%s' % (str(time.time()), str(properties)))
                    return
                quotation_data = QuotationData.get_instance()
                data = quotation_data.bar_dict
                self.logger.info('Job ' + str(1) + ' end! The time is: %s' % datetime.datetime.now())
                if strategy_context.enable_risk_control:
                    event = Event(
                        ConstantEvent.RISK_CONTROL_HANDLE_BAR,
                        context=strategy_context,
                        data=data)
                    event_bus.publish_event(event)
                event = Event(ConstantEvent.SYSTEM_HANDLE_BAR)
                event_bus.publish_event(event)
            elif body == '1':
                self.logger.info('开盘')
                if strategy_context.run_info.account_type == 0:
                    self.trade_date = DateUtil.get_next_trade_date(self.now, operate='$gte')
                else:
                    self.trade_date = DateUtil.get_next_trade_date(self.now)
                if strategy_context.enable_risk_control:
                    event = Event(
                        ConstantEvent.RISK_CONTROL_TRADING_BEFORE,
                        context=strategy_context)

                event = Event(ConstantEvent.SYSTEM_NEW_DATE)
                event_bus.publish_event(event)
            elif body == '2':
                self.logger.info('收盘')
                if strategy_context.enable_risk_control:
                    event = Event(
                        ConstantEvent.RISK_CONTROL_TRADING_AFTER,
                        context=strategy_context)
                    event_bus.publish_event(event)
                event = Event(ConstantEvent.SYSTEM_END_DATE)
                event_bus.publish_event(event)
            elif body == '3':
                self.logger.info('每天早晨开始')
                if strategy_context.enable_risk_control:
                    event = Event(
                        ConstantEvent.RISK_CONTROL_DAY_BEFORE,
                        context=strategy_context)
                    event_bus.publish_event(event)
                event = Event(ConstantEvent.SYSTEM_DAY_START)
                event_bus.publish_event(event)
            elif body == '4':
                self.logger.info('夜盘结束')
                event = Event(ConstantEvent.SYSTEM_NIGHT_END)
                event_bus.publish_event(event)
            elif body == 'risk_reload':
                run_info = strategy_context.run_info
                run_id = run_info.run_id
                event = Event(ConstantEvent.RISK_CONTROL_RELOAD, run_id=run_id, run_type= 2)
                event_bus.publish_event(event)
                if strategy_context.enable_risk_control:
                    event = Event(
                        ConstantEvent.RISK_CONTROL_INIT,
                        context=strategy_context)
                    event_bus = self.context.event_bus
                    event_bus.publish_event(event)
                self.logger.info('风控重载')
        except Exception as e:
            mes = traceback.format_exc()
            self.logger.error('rabbitmq回调异常处理，原因：%s' % str(mes))
            sr_logger = RemoteLogFactory.get_sr_logger()
            sr_logger.error(str(mes))

    def qry_account_info(self):
        event_bus = self.context.event_bus
        event = Event(ConstantEvent.SYSTEM_DAILY_DATA_SAVE)
        event_bus.publish_event(event)

    def is_stock_trade(self):
        if ('093000' <= self.hms <= '113000') or ('130000' <= self.hms <= '153000'):
            return True
        else:
            return False

    def is_future_trade(self):

        if '210000' <= self.hms <= '240000' or self.hms <= '023000' or '090000' <= self.hms <= '113000' or '130000' \
                <= self.hms <= '153000':
            return True
        else:
            return False
