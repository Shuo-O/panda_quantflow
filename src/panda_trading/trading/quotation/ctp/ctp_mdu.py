#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time   : 2019/8/5 下午5:23
# @Author : wlb
# @File   : ctp_mdu_quo.py
# @desc   :
import os
import tempfile
import threading
import hashlib
import ctp
import ctp as mdapi
import time
import traceback
import logging
from datetime import datetime
from queue import Empty, Full, Queue
from typing import Any, Dict, Optional, Tuple

import msgpack


from common.config.config import config, get_config
from panda_backtest.backtest_common.data.future.future_info_map import FutureInfoMap
from panda_backtest.backtest_common.model.quotation.bar_quotation_data import BarQuotationData
from common.connector.kafka_client import KafkaClientFactory, KafkaSettings
from common.connector.questdb_client import QuestDBClient
from common.connector.clickhouse_client import ClickHouseClient
from common.connector.mongodb_handler import DatabaseHandler as MongoClient
from common.connector.redis_client import RedisClient
from panda_trading.trading.util.symbol_util import SymbolUtil
from utils.data.data_util import DateUtil
from utils.time.time_util import TimeUtil


class MdSpi(ctp.CThostFtdcMdSpi):
    def __init__(self, front, broker_id, user_id, password):
        ctp.CThostFtdcMdSpi.__init__(self)

        self.logger = logging.getLogger(__name__)
        self.front = front
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password

        self.request_id = 0
        self.connected = False
        self.loggedin = False
        self.subscribed = False
        self.data = None

        self.api = self.create()

        # 业务字段值
        self.__redis_client = RedisClient()
        self.future_info_map = FutureInfoMap(MongoClient(config).get_mongo_db())
        self.now_trade_date_tuple = (datetime.now().strftime('%Y%m%d'),
                                     DateUtil.get_next_trade_date(datetime.now().strftime('%Y%m%d')))
        self._kafka_factory: Optional[KafkaClientFactory] = None
        self._kafka_producer = None
        self._kafka_future_tick_topic = self._get_config_value(
            "KAFKA_FUTURE_TICK_TOPIC", "market.future.tick"
        )
        self._kafka_error_logged = False
        self._kafka_ready_logged = False
        self._questdb_client = QuestDBClient.instance()
        self._clickhouse_client = ClickHouseClient.instance()
        self._enable_kafka = self._get_bool_config("ENABLE_KAFKA_TICK", True)
        self._enable_questdb = self._get_bool_config("ENABLE_QUESTDB_TICK", True)
        self._enable_clickhouse = self._get_bool_config("ENABLE_CLICKHOUSE_TICK", True)
        self._init_kafka()

        
        # 队列用于异步处理慢速落地任务（Kafka/QuestDB/ClickHouse）
        parse_queue_size = int(os.getenv("TICK_PARSE_QUEUE_SIZE", os.getenv("TICK_ASYNC_QUEUE_SIZE", "5000")))
        self._tick_queue: Queue[Dict[str, Any]] = Queue(maxsize=parse_queue_size)
        kafka_queue_size = int(os.getenv("TICK_KAFKA_QUEUE_SIZE", "5000"))
        questdb_queue_size = int(os.getenv("TICK_QUESTDB_QUEUE_SIZE", "5000"))
        clickhouse_queue_size = int(os.getenv("TICK_CLICKHOUSE_QUEUE_SIZE", "5000"))
        redis_queue_size = int(os.getenv("TICK_REDIS_QUEUE_SIZE", "10000"))
        self._redis_queue: Queue[Tuple[str, bytes]] = Queue(maxsize=redis_queue_size)
        self._redis_flush_batch = int(os.getenv("TICK_REDIS_FLUSH_BATCH", "200"))
        self._redis_flush_interval = float(os.getenv("TICK_REDIS_FLUSH_INTERVAL_MS", "5")) / 1000.0
        self._tick_stop_event = threading.Event()
        self._tick_queue_warned = False
        self._tick_worker = threading.Thread(
            target=self._tick_dispatch_loop,
            name="TickDispatch",
            daemon=True,
        )
        self._tick_worker.start()
        self._kafka_queue_warned = False
        self._questdb_queue_warned = False
        self._clickhouse_queue_warned = False
        if self._enable_kafka:
            self._kafka_queue: Queue[bytes] = Queue(maxsize=kafka_queue_size)
            self._kafka_stop_event = threading.Event()
            self._kafka_worker = threading.Thread(
                target=self._consume_kafka_queue,
                name="TickKafkaWriter",
                daemon=True,
            )
            self._kafka_worker.start()
        else:
            self._kafka_queue = None
            self._kafka_stop_event = None
            self._kafka_worker = None
        if self._enable_questdb:
            self._questdb_queue: Queue[BarQuotationData] = Queue(maxsize=questdb_queue_size)
            self._questdb_stop_event = threading.Event()
            self._questdb_worker = threading.Thread(
                target=self._consume_questdb_queue,
                name="TickQuestDBWriter",
                daemon=True,
            )
            self._questdb_worker.start()
        else:
            self._questdb_queue = None
            self._questdb_stop_event = None
            self._questdb_worker = None
        if self._enable_clickhouse:
            self._clickhouse_queue: Queue[BarQuotationData] = Queue(maxsize=clickhouse_queue_size)
            self._clickhouse_stop_event = threading.Event()
            self._clickhouse_worker = threading.Thread(
                target=self._consume_clickhouse_queue,
                name="TickClickHouseWriter",
                daemon=True,
            )
            self._clickhouse_worker.start()
        else:
            self._clickhouse_queue = None
            self._clickhouse_stop_event = None
            self._clickhouse_worker = None
        self._redis_queue_warned = False
        self._redis_stop_event = threading.Event()
        self._redis_last_flush = time.time()
        self._redis_worker = threading.Thread(
            target=self._redis_flush_loop,
            name="TickRedisWriter",
            daemon=True,
        )
        self._redis_worker.start()
        self._redis_support_pipeline = hasattr(self.__redis_client, "client") and hasattr(
            self.__redis_client.client, "pipeline"
        )

    @staticmethod
    def _get_config_value(key: str, default: str) -> str:
        cfg = get_config()
        return os.getenv(key, cfg.get(key, default))

    def _get_bool_config(self, key: str, default: bool) -> bool:
        raw_value = self._get_config_value(key, str(default))
        if isinstance(raw_value, bool):
            return raw_value
        return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}

    def _init_kafka(self):
        try:
            settings = KafkaSettings.from_env()
            if settings.is_configured():
                self._kafka_factory = KafkaClientFactory(settings)
                self._kafka_producer = self._kafka_factory.get_producer()
                if not self._kafka_ready_logged:
                    self.logger.info(
                        "[Kafka] Future tick producer ready, topic=%s, bootstrap=%s",
                        self._kafka_future_tick_topic,
                        settings.bootstrap_servers,
                    )
                    self._kafka_ready_logged = True
        except Exception as exc:
            if not self._kafka_error_logged:
                self.logger.error("[Kafka] 初始化失败: %s", exc)
                self._kafka_error_logged = True
            self._kafka_factory = None
            self._kafka_producer = None

    def _publish_kafka_tick(self, payload: bytes) -> None:
        if not self._kafka_producer or not payload:
            return
        try:
            self._kafka_producer.send(self._kafka_future_tick_topic, payload)
        except Exception as exc:
            if not self._kafka_error_logged:
                self.logger.error("[Kafka] 推送行情失败: %s", exc)
                self._kafka_error_logged = True

    def _publish_questdb_tick(self, bar_quotation_data: BarQuotationData) -> None:
        if not self._questdb_client or not bar_quotation_data.symbol:
            return
        try:
            timestamp_ns = time.time_ns()
            fields = {
                "last": bar_quotation_data.last,
                "open": bar_quotation_data.open,
                "high": bar_quotation_data.high,
                "low": bar_quotation_data.low,
                "close": bar_quotation_data.close,
                "volume": bar_quotation_data.volume,
                "turnover": bar_quotation_data.turnover,
                "oi": bar_quotation_data.oi,
                "askprice1": bar_quotation_data.askprice1,
                "bidprice1": bar_quotation_data.bidprice1,
                "askvolume1": bar_quotation_data.askvolume1,
                "bidvolume1": bar_quotation_data.bidvolume1,
                "settle": bar_quotation_data.settle,
            }
            self._questdb_client.write_tick(bar_quotation_data.symbol, fields, timestamp_ns)
        except Exception as exc:
            self.logger.debug("[QuestDB] 写入失败: %s", exc)

    def _publish_clickhouse_tick(self, bar_quotation_data: BarQuotationData) -> None:
        if not self._clickhouse_client or not bar_quotation_data.symbol:
            return
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
            row = {
                "timestamp": now,
                "symbol": bar_quotation_data.symbol,
                "last": bar_quotation_data.last,
                "open": bar_quotation_data.open,
                "high": bar_quotation_data.high,
                "low": bar_quotation_data.low,
                "close": bar_quotation_data.close,
                "volume": bar_quotation_data.volume,
                "turnover": bar_quotation_data.turnover,
                "oi": bar_quotation_data.oi,
                "askprice1": bar_quotation_data.askprice1,
                "bidprice1": bar_quotation_data.bidprice1,
                "askvolume1": bar_quotation_data.askvolume1,
                "bidvolume1": bar_quotation_data.bidvolume1,
                "settle": bar_quotation_data.settle,
            }
            self._clickhouse_client.write_tick(row)
        except Exception as exc:
            self.logger.debug("[ClickHouse] 写入失败: %s", exc)

    def _try_put_queue(
        self,
        queue_obj: Queue,
        item: Any,
        warn_attr: str,
        warn_template: str,
        symbol: Optional[str] = None,
    ) -> None:
        if queue_obj is None:
            return
        try:
            queue_obj.put_nowait(item)
        except Full:
            if not getattr(self, warn_attr):
                self.logger.warning(warn_template, symbol)
                setattr(self, warn_attr, True)

    def _snapshot_tick(self, tick: "CThostFtdcDepthMarketDataField") -> Optional[Dict[str, Any]]:
        try:
            return {
                "InstrumentID": tick.InstrumentID,
                "UpdateTime": tick.UpdateTime,
                "TradingDay": tick.TradingDay,
                "OpenPrice": tick.OpenPrice,
                "HighestPrice": tick.HighestPrice,
                "LowestPrice": tick.LowestPrice,
                "ClosePrice": tick.ClosePrice,
                "Volume": tick.Volume,
                "OpenInterest": tick.OpenInterest,
                "Turnover": tick.Turnover,
                "SettlementPrice": tick.SettlementPrice,
                "LastPrice": tick.LastPrice,
                "PreClosePrice": tick.PreClosePrice,
                "UpperLimitPrice": tick.UpperLimitPrice,
                "LowerLimitPrice": tick.LowerLimitPrice,
                "AskPrice1": tick.AskPrice1,
                "BidPrice1": tick.BidPrice1,
                "AskVolume1": tick.AskVolume1,
                "BidVolume1": tick.BidVolume1,
            }
        except Exception as exc:
            self.logger.debug("行情快照复制失败: %s", exc, exc_info=True)
            return None

    def _enqueue_tick(self, tick_snapshot: Dict[str, Any]) -> None:
        try:
            self._tick_queue.put_nowait(tick_snapshot)
        except Full:
            if not self._tick_queue_warned:
                self.logger.warning("实时行情解析队列已满，丢弃行情")
                self._tick_queue_warned = True

    def _tick_dispatch_loop(self) -> None:
        while not self._tick_stop_event.is_set():
            try:
                tick_snapshot = self._tick_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self.save_data_task(tick_snapshot)
            except Exception as exc:
                self.logger.error("处理行情快照失败: %s", exc, exc_info=True)
            finally:
                self._tick_queue.task_done()

    def _dispatch_backends(self, bar_data: BarQuotationData, payload: bytes) -> None:
        if self._enable_kafka and self._kafka_queue is not None:
            self._try_put_queue(
                self._kafka_queue,
                payload,
                "_kafka_queue_warned",
                "Kafka 队列已满，跳过行情，symbol=%s",
                bar_data.symbol,
            )
        if self._enable_questdb and self._questdb_queue is not None:
            self._try_put_queue(
                self._questdb_queue,
                bar_data,
                "_questdb_queue_warned",
                "QuestDB 队列已满，跳过行情，symbol=%s",
                bar_data.symbol,
            )
        if self._enable_clickhouse and self._clickhouse_queue is not None:
            self._try_put_queue(
                self._clickhouse_queue,
                bar_data,
                "_clickhouse_queue_warned",
                "ClickHouse 队列已满，跳过行情，symbol=%s",
                bar_data.symbol,
            )

    def _consume_kafka_queue(self) -> None:
        while not self._kafka_stop_event.is_set():
            try:
                payload = self._kafka_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._publish_kafka_tick(payload)
            except Exception as exc:
                self.logger.error("Kafka 写入失败: %s", exc, exc_info=True)
            finally:
                self._kafka_queue.task_done()

    def _consume_questdb_queue(self) -> None:
        while not self._questdb_stop_event.is_set():
            try:
                bar = self._questdb_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._publish_questdb_tick(bar)
            except Exception as exc:
                self.logger.error("QuestDB 写入失败: %s", exc, exc_info=True)
            finally:
                self._questdb_queue.task_done()

    def _consume_clickhouse_queue(self) -> None:
        while not self._clickhouse_stop_event.is_set():
            try:
                bar = self._clickhouse_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._publish_clickhouse_tick(bar)
            except Exception as exc:
                self.logger.error("ClickHouse 写入失败: %s", exc, exc_info=True)
            finally:
                self._clickhouse_queue.task_done()

    def create(self):
        dir = ''.join(('ctp', self.broker_id, self.user_id)).encode('UTF-8')
        dir = hashlib.md5(dir).hexdigest()
        dir = os.path.join(tempfile.gettempdir(), dir, 'Md') + os.sep
        if not os.path.isdir(dir): os.makedirs(dir)
        return ctp.CThostFtdcMdApi.CreateFtdcMdApi(dir)

    def run(self):
        self.api.RegisterSpi(self)
        self.api.RegisterFront(self.front)
        self.api.Init()
        self.api.Join()

    def login(self):
        field = ctp.CThostFtdcReqUserLoginField()
        field.BrokerID = self.broker_id
        field.UserID = self.user_id
        field.Password = self.password
        self.request_id += 1
        self.api.ReqUserLogin(field, self.request_id)

    def OnFrontConnected(self):
        self.logger.info("CTP 前置已连接")
        self.connected = True
        self.login()

    def OnRspUserLogin(self, pRspUserLogin:'CThostFtdcRspUserLoginField', pRspInfo:'CThostFtdcRspInfoField', nRequestID:'int', bIsLast:'bool'):
        self.logger.info("OnRspUserLogin %s %s", pRspInfo.ErrorID, pRspInfo.ErrorMsg)
        if pRspInfo.ErrorID == 0:
            self.loggedin = True

    def OnRspError(self, pRspInfo:'CThostFtdcRspInfoField', nRequestID:'int', bIsLast:'bool'):
        self.logger.error("OnRspError: %s %s", pRspInfo.ErrorID, pRspInfo.ErrorMsg)

    def OnRspSubMarketData(self, pSpecificInstrument: 'CThostFtdcSpecificInstrumentField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
        self.logger.info("OnRspSubMarketData: %s %s", pRspInfo.ErrorID, pRspInfo.ErrorMsg)
        if pRspInfo.ErrorID == 0:
            self.subscribed = True

    def OnRtnDepthMarketData(self, pDepthMarketData: 'CThostFtdcDepthMarketDataField'):
        """
        收到行情推送时的回调函数
        """
        self.logger.debug(
            "Tick %s last=%s bid1=%s ask1=%s",
            pDepthMarketData.InstrumentID,
            pDepthMarketData.LastPrice,
            pDepthMarketData.BidPrice1,
            pDepthMarketData.AskPrice1,
        )
        self.data = pDepthMarketData
        # self.save_data_task(pDepthMarketData)
        snapshot = self._snapshot_tick(pDepthMarketData)
        if snapshot:
            self._enqueue_tick(snapshot)


    def save_data_task(self, tick_snapshot: Dict[str, Any]) -> None:
        try:
            if tick_snapshot is None:
                self.logger.warning('行情推送数据为空')
                return
            # 实时链路仅负责生成行情快照并写入 Redis，其他慢操作交给异步线程处理
            bar_quotation_data = self.depth_market_dat_to_symbol(tick_snapshot)
            if bar_quotation_data is None:
                return
            key = bar_quotation_data.symbol
            payload = self._serialize_bar(bar_quotation_data)
            self._enqueue_redis_update(key, payload)
            self._dispatch_backends(bar_quotation_data, payload)
        except Exception as e:
            mes = traceback.format_exc()
            self.logger.exception("保存行情数据异常: %s", mes)

    def depth_market_dat_to_symbol(self, tick_data: Dict[str, Any]):
        try:
            bar_quotation_data = BarQuotationData()
            instrument_id = (tick_data.get("InstrumentID") or "").strip()
            symbol_info = self.future_info_map.get_by_ctp_code(instrument_id)
            if symbol_info:
                bar_quotation_data.symbol = symbol_info['symbol']
            else:
                bar_quotation_data.symbol = instrument_id
            # bar_quotation_data.code = bar_data['code']
            bar_quotation_data.date = datetime.now().strftime('%Y%m%d')
            bar_quotation_data.time = tick_data.get("UpdateTime")
            bar_quotation_data.trade_date = tick_data.get("TradingDay")
            bar_quotation_data.open = tick_data.get("OpenPrice")
            bar_quotation_data.high = tick_data.get("HighestPrice")
            bar_quotation_data.low = tick_data.get("LowestPrice")
            bar_quotation_data.close = tick_data.get("ClosePrice")
            bar_quotation_data.volume = tick_data.get("Volume")
            bar_quotation_data.oi = tick_data.get("OpenInterest")
            bar_quotation_data.turnover = tick_data.get("Turnover")
            # bar_quotation_data.vwap = bar_data['vwap']
            # bar_quotation_data.oi = bar_data['oi']
            bar_quotation_data.settle = tick_data.get("SettlementPrice")
            bar_quotation_data.last = tick_data.get("LastPrice")
            bar_quotation_data.preclose = tick_data.get("PreClosePrice")
            bar_quotation_data.limit_up = tick_data.get("UpperLimitPrice")
            bar_quotation_data.limit_down = tick_data.get("LowerLimitPrice")
            bar_quotation_data.askprice1 = tick_data.get("AskPrice1")
            bar_quotation_data.bidprice1 = tick_data.get("BidPrice1")
            bar_quotation_data.askvolume1 = tick_data.get("AskVolume1")
            bar_quotation_data.bidvolume1 = tick_data.get("BidVolume1")
            parts = bar_quotation_data.symbol.split('.')
            if len(parts) >= 2:
                exchange = parts[1]
            else:
                # 如果没有找到 '.'，可以设置默认值或处理异常情况
                exchange = None  # 或者 raise ValueError 或者使用其他默认逻辑
                self.logger.warning("Invalid symbol format %s", bar_quotation_data.symbol)
            if exchange == 'CZC':
                if TimeUtil.in_time_range('210000-235959'):
                    if datetime.now().strftime('%Y%m%d') == self.now_trade_date_tuple[0]:
                        bar_quotation_data.trade_date = self.now_trade_date_tuple[1]
                    else:
                        self.now_trade_date_tuple = (datetime.now().strftime('%Y%m%d'),
                                                     DateUtil.get_next_trade_date(datetime.now().strftime('%Y%m%d')))
                        bar_quotation_data.trade_date = self.now_trade_date_tuple[1]
                elif TimeUtil.in_time_range('000000-023000'):
                    if datetime.now().strftime('%Y%m%d') == self.now_trade_date_tuple[0]:
                        bar_quotation_data.trade_date = self.now_trade_date_tuple[1]
                    else:
                        self.now_trade_date_tuple = (datetime.now().strftime('%Y%m%d'),
                                                     DateUtil.get_next_trade_date(datetime.now().strftime('%Y%m%d'),
                                                                                  operate='$gte'))
                        bar_quotation_data.trade_date = self.now_trade_date_tuple[1]

            return bar_quotation_data
        except Exception as e:
            mes = traceback.format_exc()
            self.logger.exception('depth_market_dat_to_symbol异常：%s', mes)
            return None

    @staticmethod
    def _serialize_bar(bar_data: BarQuotationData) -> bytes:
        return msgpack.packb(bar_data.__dict__, use_bin_type=True)

    def _enqueue_redis_update(self, symbol: str, payload: bytes) -> None:
        if not symbol:
            return
        try:
            self._redis_queue.put_nowait((symbol, payload))
        except Full:
            if not self._redis_queue_warned:
                self.logger.warning("Redis 队列已满，跳过行情写入，symbol=%s", symbol)
                self._redis_queue_warned = True

    def _redis_flush_loop(self) -> None:
        cache: Dict[str, bytes] = {}
        while not self._redis_stop_event.is_set() or not self._redis_queue.empty():
            flushed = False
            try:
                symbol, payload = self._redis_queue.get(timeout=0.001)
                cache[symbol] = payload
                self._redis_queue.task_done()
            except Empty:
                pass

            now = time.time()
            if cache and (
                len(cache) >= self._redis_flush_batch
                or (now - self._redis_last_flush) >= self._redis_flush_interval
            ):
                self._flush_redis_batch(cache)
                cache.clear()
                self._redis_last_flush = now
                flushed = True

            if flushed:
                continue

        if cache:
            self._flush_redis_batch(cache)

    def _flush_redis_batch(self, data: Dict[str, bytes]) -> None:
        if not data:
            return
        try:
            if self._redis_support_pipeline:
                pipeline = self.__redis_client.client.pipeline(transaction=False)
                for symbol, payload in data.items():
                    pipeline.hset('tushare_future_tick_quotation', symbol, payload)
                pipeline.execute()
            else:
                for symbol, payload in data.items():
                    self.__redis_client.setHashRedis('tushare_future_tick_quotation', symbol, payload)
        except Exception as exc:
            self.logger.error("批量写入 Redis 失败: %s", exc, exc_info=True)

    def __del__(self):
        try:
            self._tick_stop_event.set()
            if hasattr(self, "_tick_worker") and self._tick_worker and self._tick_worker.is_alive():
                self._tick_worker.join(timeout=1.0)
            if self._enable_kafka and self._kafka_stop_event:
                self._kafka_stop_event.set()
                if self._kafka_worker and self._kafka_worker.is_alive():
                    self._kafka_worker.join(timeout=1.0)
            if self._enable_questdb and self._questdb_stop_event:
                self._questdb_stop_event.set()
                if self._questdb_worker and self._questdb_worker.is_alive():
                    self._questdb_worker.join(timeout=1.0)
            if self._enable_clickhouse and self._clickhouse_stop_event:
                self._clickhouse_stop_event.set()
                if self._clickhouse_worker and self._clickhouse_worker.is_alive():
                    self._clickhouse_worker.join(timeout=1.0)
            self._redis_stop_event.set()
            if self._redis_worker and self._redis_worker.is_alive():
                self._redis_worker.join(timeout=1.0)
        except Exception:
            pass
        self.api.RegisterSpi(None)
        self.api.Release()

def spi(front, broker, user, password):
    assert front and broker and user and password, "missing arguments"
    _spi = MdSpi(front, broker, user, password)
    th = threading.Thread(target=_spi.run)
    th.daemon = True
    th.start()
    secs = 5
    while secs:
        if not (_spi.connected and _spi.loggedin):
            secs -= 1
            time.sleep(1)
        else:
            break
    return _spi

def get_future_list()->list:
    future_codes = list()
    trade_date = datetime.now().strftime('%Y%m%d')
    future_code_list = SymbolUtil.get_future_code_and_type_list(trade_date)[0]
    for future_code in future_code_list:
        future_codes.append(SymbolUtil.symbol_to_ctp_code(future_code))
    return future_codes

def main():
    # "userid": "242943",
    # "password": "20252025Wld~~",
    # "brokerid": "9999",
    # "md_address": "tcp://180.168.146.187:10211",
    # "appid": "simnow_client_test",
    # "auth_code": "0000000000000000"

    _spi = spi("tcp://182.254.243.31:30011","9999", "242943", "20252025Wld~~")
    # _spi = spi("tcp://180.168.146.187:10211","9999", "242943", "20252025Wld~~")

    codes = get_future_list()
    # ["rb2510"]
    # print(codes[1])
    _spi.api.SubscribeMarketData(codes)
    # _spi.api.SubscribeMarketData(["rb2510"])

    secs = 15
    while secs:
        continue

if __name__ == '__main__':
    main()
