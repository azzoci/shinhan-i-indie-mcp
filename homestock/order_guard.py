from __future__ import annotations

from .models import OrderRequest, OrderResult


class OrderGuard:
    def __init__(self, allow_live_orders: bool) -> None:
        self.allow_live_orders = allow_live_orders

    def block_if_needed(self, request: OrderRequest, action: str) -> OrderResult | None:
        if self.allow_live_orders:
            return None

        return OrderResult(
            accepted=False,
            live_order=False,
            order_id=None,
            message=f"{action} blocked because ALLOW_LIVE_ORDERS is false",
            raw={
                "account_no": request.account_no,
                "code": request.code,
                "side": request.side,
                "quantity": request.quantity,
                "price": request.price,
                "order_type": request.order_type,
                "original_order_id": request.original_order_id,
            },
        )

