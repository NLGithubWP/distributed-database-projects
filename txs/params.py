

class NewOrderTxParams:
    def __init__(self):
        self.c_id: int = 0
        self.w_id: int = 0
        self.d_id: int = 0
        self.num_items: int = 0
        self.item_number: [int] = []
        self.supplier_warehouse: [int] = []
        self.quantity: [int] = []


class PaymentTxParams:
    def __init__(self):
        self.c_w_id: int = 0
        self.c_d_id: int = 0
        self.c_id: int = 0
        self.payment_amount: float = 0.0


class DeliveryTxParams:
    def __init__(self):
        self.w_id: int = 0
        self.carrier_id: int = 0


class OrderStatusTxParams:
    def __init__(self):
        self.c_w_id: int = 0
        self.c_d_id: int = 0
        self.c_id: int = 0


class StockLevelTxParams:
    def __init__(self):
        self.w_id: int = 0
        self.d_id: int = 0
        self.threshold: int = 0
        self.l: int = 0


class PopItemTxParams:
    def __init__(self):
        self.w_id: int = 0
        self.d_id: int = 0
        self.l: int = 0


class TopBalanceTxParams:
    pass


class RelCustomerTxParams:
    def __init__(self):
        self.w_id: int = 0
        self.d_id: int = 0
        self.c_id: int = 0
