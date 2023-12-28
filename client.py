import asyncio
import json
import websockets

from typing import Any

from time import time
from random import randint


class StockExchangeException(Exception):
    """
    An error thrown by ExchangeClient instances
    """


class ExchangeListener:
    def __init__(self):
        self._url = "wss://192.168.1.34:8001"
        self._connection = None

    async def open_connection(self, username: str, password: str):
        self._connection = await websockets.connect(self._url)
        await self._connection.send(
            json.dumps(
                {"username": username, "password": password, "listener": "constant"}
            )
        )
        await asyncio.create_task(self.listen_forever())

    async def listen_forever(self):
        while True:
            yield self._connection.recv()

    async def get_next_events(self) -> list[str]:
        res = []
        while (
                next_val := await asyncio.wait_for(
                    await anext(
                        self.listen_forever()
                    ),
                    timeout=1.0
                )
        ) is not None:
            res.append(next_val)
        return res


class ExchangeClient:
    def __init__(self, server_ip: str):
        self._url = f"ws://{server_ip}:8001"
        self._connection = None
        self._listener = ExchangeListener()
        self.sent_orders = set()

    async def __aenter__(self):
        self._connection = await websockets.connect(self._url)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._connection.close()

    async def open_connection(self):
        self._connection = await websockets.connect(self._url)

    async def close_connection(self):
        await self._connection.close()

    def _parse_message(self, msg: str) -> Any:
        msg_dict = json.loads(msg)
        if "status" not in msg_dict or "result" not in msg_dict:
            raise StockExchangeException(f"Invalid return format from exchange: {msg_dict}")
        elif msg_dict["status"] != 200:
            raise StockExchangeException(f"Error code returned from Exchange with status code: "
                                         f"{msg_dict['status']} and message {msg_dict['result']}")

        # Handle any info that depends on the returned method
        if "method" in msg_dict:
            if msg_dict["method"] in ("bid", "ask"):
                # Use received messages to update the set of sent orders
                self.sent_orders.add(msg_dict["method"])
            elif msg_dict["method"] == "cancel" and msg_dict["result"] is True:
                self.sent_orders.remove(msg_dict["method"])
        else:
            raise StockExchangeException(f"Invalid return format from exchange: {msg_dict}")

        return msg_dict["result"]

    async def _send_message(self, msg_json: dict) -> Any:
        msg_json = json.dumps(msg_json)
        if self._connection is None:
            self._connection = await websockets.connect(self._url)

        await self._connection.send(msg_json)
        msg = await self._connection.recv()
        return self._parse_message(msg)

    async def login(self, username: str, password: str) -> bool:
        # await self._listener.open_connection(username, password)
        return await self._send_message(
            {"username": username, "password": password}
        )

    async def bid(self, quantity: int, price: int) -> int:
        return await self._send_message(
            {"method": "bid", "params": [quantity, price, "limit"]}
        )

    async def ask(self, quantity: int, price: int) -> int:
        return await self._send_message(
            {"method": "ask", "params": [quantity, price, "limit"]}
        )

    async def cancel(self, order_id: int) -> bool:
        return await self._send_message(
            {"method": "cancel", "params": [order_id]}
        )

    async def update(self, order_id: int, quantity: int) -> bool:
        return await self._send_message(
            {"method": "update", "params": [order_id, quantity]}
        )

    async def print_book(self):
        print(await self._send_message({"method": "print"}))

    async def print_executed_transactions(self):
        print((await self._send_message({"method": "print_transactions"})))

    async def logout(self):
        await self._send_message(
            {"method": "logout"}
        )


async def make_bids():
    exc = ExchangeClient("192.168.1.34")
    await exc.login("gmandonnaneeeee", "benis")
    t0 = time()
    for i in range(10_000):
        await exc.bid(randint(1, 100), randint(1, 100))
        await exc.ask(randint(1, 100), randint(5, 105))
    print(time() - t0)
    await exc.print_executed_transactions()
    await exc.close_connection()


if __name__ == "__main__":
    asyncio.run(make_bids())
