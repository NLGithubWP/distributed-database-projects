
from . import *
from .params import *
from abc import abstractmethod


class Transactions(object):

    @abstractmethod
    def new_order_transaction(self, m_conn, m_params: NewOrderTxParams):
        raise NotImplementedError

    @abstractmethod
    def payment_transaction(self, m_conn, m_params: PaymentTxName):
        raise NotImplementedError

    @abstractmethod
    def delivery_transaction(self, m_conn, m_params: DeliveryTxParams):
        raise NotImplementedError

    @abstractmethod
    def order_status_transaction(self, m_conn, m_params: OrderStatusTxParams):
        raise NotImplementedError

    @abstractmethod
    def stock_level_transaction(self, m_conn, m_params: StockLevelTxParams):
        raise NotImplementedError

    @abstractmethod
    def popular_item_transaction(self, m_conn, m_params: PopItemTxParams):
        raise NotImplementedError

    @abstractmethod
    def top_balance_transaction(self, m_conn):
        raise NotImplementedError

    @abstractmethod
    def related_customer_transaction(self, m_conn, m_params: RelCustomerTxParams):
        raise NotImplementedError

