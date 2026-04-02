from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Any
from uuid import uuid4

from .models import ProductInfo, TradeResult


class ExchangeError(RuntimeError):
    pass


def _to_dict(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "to_dict"):
        return payload.to_dict()
    if isinstance(payload, dict):
        return payload
    raise ExchangeError(f"Unexpected exchange response type: {type(payload)!r}")


def _quantize(value: float, increment: float) -> str:
    quantized = Decimal(str(value)).quantize(Decimal(str(increment)), rounding=ROUND_DOWN)
    return format(quantized.normalize(), "f")


class CoinbaseLiveTrader:
    def __init__(self, api_key: str, api_secret: str) -> None:
        try:
            from coinbase.rest import RESTClient
        except ImportError as exc:
            raise ExchangeError("coinbase-advanced-py is not installed.") from exc

        self.client = RESTClient(api_key=api_key, api_secret=api_secret, timeout=10)

    def place_market_buy(self, product: ProductInfo, quote_size: float) -> TradeResult:
        payload = {
            "client_order_id": uuid4().hex,
            "product_id": product.product_id,
            "side": "BUY",
            "order_configuration": {
                "market_market_ioc": {
                    "quote_size": _quantize(quote_size, product.quote_increment),
                }
            },
        }
        response = _to_dict(self.client.post("/api/v3/brokerage/orders", data=payload))
        return self._trade_result_from_response(response)

    def place_market_sell(self, product: ProductInfo, base_size: float) -> TradeResult:
        payload = {
            "client_order_id": uuid4().hex,
            "product_id": product.product_id,
            "side": "SELL",
            "order_configuration": {
                "market_market_ioc": {
                    "base_size": _quantize(base_size, product.base_increment),
                }
            },
        }
        response = _to_dict(self.client.post("/api/v3/brokerage/orders", data=payload))
        return self._trade_result_from_response(response)

    def _trade_result_from_response(self, response: dict[str, Any]) -> TradeResult:
        if not response.get("success"):
            error = response.get("error_response") or response
            raise ExchangeError(f"Exchange order rejected: {error}")

        order_id = response["success_response"]["order_id"]
        order_details = _to_dict(self.client.get(f"/api/v3/brokerage/orders/historical/{order_id}"))
        order = order_details.get("order", order_details)

        filled_size = float(order.get("filled_size") or 0.0)
        average_filled_price = float(order.get("average_filled_price") or 0.0)
        fees_paid = float(order.get("total_fees") or order.get("fee") or 0.0)
        if filled_size <= 0 or average_filled_price <= 0:
            raise ExchangeError(
                "Order was accepted but fill details are unavailable. "
                "Check the Coinbase dashboard before sending another order."
            )

        return TradeResult(
            price=average_filled_price,
            quantity=filled_size,
            fees_paid=fees_paid,
            order_id=order_id,
        )
