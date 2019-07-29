# code: utf-8

from enum import unique, IntEnum


@unique
class Direction(IntEnum):
    Buy = 1
    Sell = -1

    def flap(self):
        return self.__class__(self.value * -1)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


def new_direction(value) -> Direction:
    """
    Create direction enum by value or name,
    name string can be either lower-case or capitalized.
    :param value: direction value or name
    :return: Direction enum
    """

    if isinstance(value, int):
        return Direction(value)

    if isinstance(value, str):
        try:
            return getattr(Direction, "__members__")[value.capitalize()]
        except KeyError:
            pass

    raise ValueError("invalid direction: {}".format(value))


@unique
class OrderStatus(IntEnum):
    Canceled = -1
    New = 0
    PartiallyFilled = 1
    PartiallyFilledCanceled = 2
    Filled = 3
    Rejected = 255

    def is_initial(self) -> bool:
        return self in (OrderStatus.New,)

    def is_progress(self) -> bool:
        return self in (OrderStatus.PartiallyFilled,)

    def is_finished(self) -> bool:
        return self in (OrderStatus.Canceled,
                        OrderStatus.PartiallyFilledCanceled,
                        OrderStatus.Filled, OrderStatus.Rejected)

    def migrate(self, status):
        if self.is_finished() or status.is_initial():
            return None

        if status.is_progress():
            return status

        if self.is_initial() and status not in (
                OrderStatus.PartiallyFilledCanceled,):
            return status

        if self.is_progress() and status not in (
                OrderStatus.Canceled, OrderStatus.Rejected):
            return status

        return None

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class OrderType(IntEnum):
    """
    当委托价格存在时，默认为限价单
    当停止价格存在时，默认为市价止损单
    当委托价及停止价同时存在时，默认为限价止损单
    """

    # 市价单
    Market = 1

    # 限价单
    Limit = 2

    # 市价止损单
    Stop = 3

    # 限价止损单
    StopLimit = 4

    # 市价止盈单
    MarketIfTouched = 5

    # 限价止盈单
    LimitIfTouched = 6

    # 市价转限价单 不懂什么含义……
    MarketWithLeftOverAsLimit = 7

    # 追踪单
    Pegged = 8

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class TimeCondition(IntEnum):
    Day = 1
    GoodTillCancel = 2
    ImmediateOrCancel = 3
    FillOrKill = 4

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


def create_enum_by_name(cls, name):
    if isinstance(name, str):
        try:
            return getattr(cls, "__members__")[name]
        except KeyError:
            pass

    raise ValueError("invalid {}: {}".format(cls, name))
