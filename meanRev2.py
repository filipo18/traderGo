# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools
import random

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS


# noinspection DuplicatedCode
class AutoTrader(BaseAutoTrader):
    """First try

    Will use a pairs trading strategy to trade the two instruments. If the ASK
    price of one is smaller than the BID price of the other, then we will order
    the first and sell the second. If the ASK price of one is larger than the
    BID price of the other, then we will sell the first and buy the second. The
    transaction will be exectued first on market with the lower liquidity.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        self.lastEtfBids = self.lastEtfAsks = self.lastFutureBids = self.lastFutureAsks = ((0, 0), (0, 0))

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)
        if instrument == Instrument.ETF:
            self.lastEtfBids = tuple(zip(bid_prices, bid_volumes))
            self.lastEtfAsks = tuple(zip(ask_prices, ask_volumes))
        else:
            self.lastFutureBids = tuple(zip(bid_prices, bid_volumes))
            self.lastFutureAsks = tuple(zip(ask_prices, ask_volumes))

        (etf_price, volume, side, fut_price) = self.get_max_possible_trade()
        if etf_price != 0 and volume != 0:
            if side == Side.ASK:
                self.ask_price = etf_price
                self.ask_id = next(self.order_ids)
                self.send_insert_order(self.ask_id, side, etf_price, volume, Lifespan.F)
                self.send_hedge_order(next(self.order_ids), Side.BID, fut_price, volume)
            else:
                self.bid_price = etf_price
                self.bid_id = next(self.order_ids)
                self.send_insert_order(self.bid_id, side, etf_price, volume, Lifespan.F)
                self.send_hedge_order(next(self.order_ids), Side.ASK, fut_price, volume)



    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)
        if client_order_id in self.bids:
            self.position += volume
        elif client_order_id in self.asks:
            self.position -= volume

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)

    def get_max_possible_trade(self) -> (int, int, int, int):  # (price, volume, side, future_price)
        """ Returns maximum possible trade price, volume, side for ETF using simple logic.

        Logic:
        We check what is the maximum volume at which we can buy ETF for cheaper, and sell futures for more.
        Maximum value is found by checking every price until we find a price where we
        can't buy ETF for cheaper

        """
        max_volume = 0
        etf_price = 0
        fut_price = 0
        if self.lastEtfAsks[0][0] < self.lastFutureBids[0][0]:  # Check for trade opportunity
            for ask in self.lastEtfAsks:  # Find max volume and limit for the trade
                for bid in self.lastFutureBids:
                    if ask[0] < bid[0]:
                        max_volume += min(ask[1], bid[1])  # MISTAKE
                        etf_price = ask[0]  # we buy by the price of ask
                        fut_price = bid[0]  # we sell by the price of bid
            return etf_price, max_volume, Side.BID, fut_price  # buy etf, sell fut
        elif self.lastEtfBids[0][0] > self.lastFutureAsks[0][0]:
            for ask in self.lastFutureAsks:
                for bid in self.lastEtfBids:
                    if ask[0] < bid[0]:
                        max_volume += min(ask[1], bid[1])  # MISTAKE
                        etf_price = bid[0]
                        fut_price = ask[0]
            return etf_price, max_volume, Side.ASK, fut_price  # sell etf, buy fut
        return 0, 0, Side.BID, 0
