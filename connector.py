"""MarginEdge Public API Connector for Fivetran.

Syncs restaurant, order, product, vendor, and category data from the MarginEdge
Public API (https://api.marginedge.com/public) into a destination warehouse.
MarginEdge is a back-of-house restaurant operations platform (invoicing,
inventory, vendor management).

IMPORTANT - UNVERIFIED DETAILS:
This connector was built from a third-party OpenAPI mirror of the MarginEdge
Public API. MarginEdge's official documentation site blocked automated fetch
during code generation, so the following details could NOT be confirmed
against live docs and MUST be verified during local testing with real
credentials before relying on this connector in production:
  - Whether `GET /orders` filters by `createdDate` vs. `invoiceDate` (this
    connector assumes `createdDate` is both the filter semantics for
    `startDate`/`endDate` and the incremental cursor field).
  - The exact `orderStatus` enum values. This connector intentionally omits
    the `orderStatus` filter entirely and syncs all orders regardless of
    status, so this uncertainty should not affect correctness, only means
    status-based filtering is unavailable.
  - The exact JSON field names for `attachments[]` entries on order detail
    (`attachmentId` / `attachmentUrl` are best-guess names).
  - Whether list endpoints wrap results in a `data` array alongside the
    `nextPage` cursor, or return a bare array. This connector handles both
    shapes defensively (see `_extract_list`).

See README.md for setup instructions.
"""

import time
from datetime import date, datetime, timedelta, timezone

import requests as rq

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.marginedge.com/public"
__REQUEST_TIMEOUT = 30  # seconds
__MAX_RETRIES = 5
__BASE_DELAY = 1  # seconds, base for exponential backoff
__MAX_DELAY = 60  # seconds, cap for exponential backoff
__REQUEST_DELAY = 0.15  # seconds, defensive fixed delay between every request
__ORDER_WINDOW_DAYS = 30  # size of each incremental orders date window


def validate_configuration(configuration: dict):
    """Ensure all required configuration values are present before syncing.

    Args:
        configuration: dictionary containing connection details.
    Raises:
        ValueError: if any required configuration value is missing.
    """
    required_keys = ["api_key"]
    for key in required_keys:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")


def _get_headers(configuration: dict) -> dict:
    """Build request headers carrying the MarginEdge API key.

    Args:
        configuration: connector configuration dict containing `api_key`.
    Returns:
        Header dict with the `x-api-key` header set.
    """
    return {"x-api-key": configuration["api_key"], "Accept": "application/json"}


def _is_retryable_status(status_code: int) -> bool:
    """Return True if the HTTP status code should be retried with backoff."""
    return status_code == 429 or 500 <= status_code < 600


def _get(configuration: dict, path: str, params: dict = None):
    """Issue a single GET request to the MarginEdge API with retry/backoff handling.

    - 429 and 5xx responses are retried with capped exponential backoff
      (base 1s, up to __MAX_RETRIES retries, capped at __MAX_DELAY seconds).
    - 403 responses raise immediately (invalid key or restaurant unit not
      authorized for this key) - this is not retryable.
    - 400 responses raise immediately (malformed request params) rather than
      being silently skipped.
    - 404 responses return None so the caller can log-and-skip that record.
    - A small fixed delay is applied before every request as a defensive
      courtesy since MarginEdge does not document a numeric rate limit.

    Args:
        configuration: connector configuration dict (used for auth headers).
        path: API path relative to the base URL, e.g. "/orders".
        params: optional query string parameters.
    Returns:
        Parsed JSON response body, or None if the resource returned HTTP 404.
    Raises:
        requests.HTTPError: for 400, 403, or retry-exhausted failures.
    """
    url = f"{__BASE_URL}{path}"
    headers = _get_headers(configuration)
    attempt = 0

    while True:
        time.sleep(__REQUEST_DELAY)
        try:
            response = rq.get(url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT)
        except (rq.exceptions.ConnectionError, rq.exceptions.Timeout) as exc:
            if attempt >= __MAX_RETRIES:
                log.error(f"Network error calling {path} after {attempt} retries: {exc}")
                raise
            delay = min(__MAX_DELAY, __BASE_DELAY * (2 ** attempt))
            log.warning(f"Network error calling {path}, retrying in {delay}s (attempt {attempt + 1}/{__MAX_RETRIES}): {exc}")
            time.sleep(delay)
            attempt += 1
            continue

        if response.status_code == 200:
            return response.json()

        if response.status_code == 400:
            log.error(f"Bad request (400) calling {path} with params={params}")
            response.raise_for_status()

        if response.status_code == 403:
            log.error(f"Forbidden (403) calling {path}: invalid API key or restaurant unit not authorized for this key")
            response.raise_for_status()

        if response.status_code == 404:
            log.warning(f"Not found (404) calling {path} with params={params}; skipping this record")
            return None

        if _is_retryable_status(response.status_code):
            if attempt >= __MAX_RETRIES:
                log.error(f"Exhausted retries calling {path}: HTTP {response.status_code}")
                response.raise_for_status()
            delay = min(__MAX_DELAY, __BASE_DELAY * (2 ** attempt))
            log.warning(
                f"HTTP {response.status_code} calling {path}, retrying in {delay}s "
                f"(attempt {attempt + 1}/{__MAX_RETRIES})"
            )
            time.sleep(delay)
            attempt += 1
            continue

        # Any other unexpected status code - raise.
        response.raise_for_status()


def _extract_list(body) -> list:
    """Normalize a MarginEdge list-endpoint response body into a list of records.

    Handles both a bare JSON array and an object wrapping the array under a
    `data` key (the exact wrapper shape is not fully documented, so both are
    supported defensively).
    """
    if body is None:
        return []
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return body.get("data") or []
    return []


def _paginated_get(configuration: dict, path: str, params: dict = None):
    """Yield every record from a MarginEdge list endpoint, following the `nextPage` cursor.

    MarginEdge list endpoints return an opaque `nextPage` field in the
    response when more pages exist; it is echoed back as a `nextPage` query
    parameter on the next request. Absence of `nextPage` means the last page
    has been reached. This also works transparently for endpoints that never
    return a `nextPage` field (e.g. single-page lists) - the loop simply ends
    after the first page.

    Args:
        configuration: connector configuration dict.
        path: API path relative to the base URL.
        params: base query parameters (a `nextPage` value is added automatically
            on subsequent requests).
    Yields:
        Each record (dict) returned by the endpoint, across all pages.
    """
    base_params = dict(params or {})
    query = dict(base_params)
    while True:
        body = _get(configuration, path, query)
        if body is None:
            return
        for item in _extract_list(body):
            yield item
        next_page = body.get("nextPage") if isinstance(body, dict) else None
        if not next_page:
            return
        query = dict(base_params)
        query["nextPage"] = next_page


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_windows(start: date, end: date, window_days: int):
    """Yield consecutive (window_start, window_end) date pairs covering [start, end)."""
    current = start
    while current < end:
        window_end = min(current + timedelta(days=window_days), end)
        yield current, window_end
        current = window_end


def schema(configuration: dict):
    """Declare all tables and primary keys synced by this connector.

    Column types are intentionally left undeclared everywhere possible so the
    SDK can infer types and the schema can evolve as the MarginEdge API
    changes; only `table` and `primary_key` are specified.

    Args:
        configuration: connector configuration dict (unused here, but part of
            the required function signature).
    Returns:
        List of table definitions.
    """
    return [
        {"table": "restaurant_units", "primary_key": ["id"]},
        {"table": "restaurant_unit_groups", "primary_key": ["id"]},
        {"table": "restaurant_unit_group_units", "primary_key": ["group_id", "unit_id"]},
        {"table": "restaurant_unit_group_categories", "primary_key": ["id"]},
        {"table": "orders", "primary_key": ["restaurant_unit_id", "order_id"]},
        {
            "table": "order_line_items",
            "primary_key": ["restaurant_unit_id", "order_id", "line_index"],
        },
        {
            "table": "order_attachments",
            "primary_key": ["restaurant_unit_id", "order_id", "attachment_id"],
        },
        {
            "table": "products",
            "primary_key": ["restaurant_unit_id", "company_concept_product_id"],
        },
        {
            "table": "product_categories",
            "primary_key": ["restaurant_unit_id", "company_concept_product_id", "category_id"],
        },
        {"table": "vendors", "primary_key": ["restaurant_unit_id", "vendor_id"]},
        {
            "table": "vendor_accounts",
            "primary_key": ["restaurant_unit_id", "vendor_id", "account_index"],
        },
        {
            "table": "vendor_items",
            "primary_key": ["restaurant_unit_id", "vendor_id", "vendor_item_code"],
        },
        {
            "table": "vendor_item_packaging",
            "primary_key": ["restaurant_unit_id", "vendor_id", "vendor_item_code", "packaging_id"],
        },
        {"table": "categories", "primary_key": ["restaurant_unit_id", "category_id"]},
    ]


def _sync_restaurant_units(configuration: dict) -> list:
    """Fetch and upsert the restaurant_units table.

    This is the root of every other fan-out loop, so the returned list is
    cached and reused by the caller rather than being re-fetched per table.

    Returns:
        List of raw restaurant unit dicts (with at least `id` and `name`).
    """
    units = []
    for unit in _paginated_get(configuration, "/restaurantUnits"):
        units.append(unit)
        op.upsert("restaurant_units", {"id": unit.get("id"), "name": unit.get("name")})
    return units


def _sync_restaurant_unit_groups(configuration: dict):
    """Fetch and upsert restaurant_unit_groups and their nested unit memberships."""
    for group in _paginated_get(configuration, "/restaurantUnits/groups"):
        group_id = group.get("id")
        op.upsert(
            "restaurant_unit_groups",
            {
                "id": group_id,
                "name": group.get("name"),
                "company_id": group.get("companyId"),
                "company_name": group.get("companyName"),
                "concept_id": group.get("conceptId"),
                "concept_name": group.get("conceptName"),
                "group_category_id": group.get("groupCategoryId"),
                "group_category_name": group.get("groupCategoryName"),
                "last_modified_date": group.get("lastModifiedDate"),
            },
        )
        for unit in group.get("units", []) or []:
            op.upsert(
                "restaurant_unit_group_units",
                {
                    "group_id": group_id,
                    "unit_id": unit.get("unitId"),
                    "unit_name": unit.get("unitName"),
                },
            )


def _sync_restaurant_unit_group_categories(configuration: dict):
    """Fetch and upsert restaurant_unit_group_categories."""
    for category in _paginated_get(configuration, "/restaurantUnits/groupCategories"):
        op.upsert(
            "restaurant_unit_group_categories",
            {
                "id": category.get("id"),
                "name": category.get("name"),
                "company_id": category.get("companyId"),
                "company_name": category.get("companyName"),
                "concept_id": category.get("conceptId"),
                "concept_name": category.get("conceptName"),
                "permission": category.get("permission"),
            },
        )


def _sync_one_order(configuration: dict, unit_id, order: dict):
    """Upsert one order's list-level fields, enrich with order-detail fields, and
    flatten its nested line items and attachments.

    If the order-detail fetch returns HTTP 404 (unknown orderId), the
    list-level record is still upserted, but detail-only fields and child rows
    (line items, attachments) are skipped and a warning is logged, per the
    "log and skip that record" policy for 404s on this endpoint.

    Args:
        configuration: connector configuration dict.
        unit_id: the restaurant unit ID this order belongs to.
        order: the raw order dict from the /orders list endpoint.
    """
    order_id = order.get("orderId")
    if order_id is None:
        log.warning(f"Skipping order with missing orderId for unit {unit_id}: {order}")
        return

    record = {
        "restaurant_unit_id": unit_id,
        "order_id": order_id,
        "created_date": order.get("createdDate"),
        "invoice_number": order.get("invoiceNumber"),
        "vendor_id": order.get("vendorId"),
        "vendor_name": order.get("vendorName"),
        "customer_number": order.get("customerNumber"),
        "invoice_date": order.get("invoiceDate"),
        "payment_account": order.get("paymentAccount"),
        "order_total": order.get("orderTotal"),
        "status": order.get("status"),
    }

    detail = _get(configuration, f"/orders/{order_id}", {"restaurantUnitId": unit_id})
    if detail is None:
        log.warning(f"Order detail not found (404) for orderId={order_id}, unit={unit_id}; upserting list fields only")
        op.upsert("orders", record)
        return

    record.update(
        {
            "delivery_charges": detail.get("deliveryCharges"),
            "other_charges": detail.get("otherCharges"),
            "tax": detail.get("tax"),
            "input_tax_credits": detail.get("inputTaxCredits"),
            "credit_amount": detail.get("creditAmount"),
            "is_credit": detail.get("isCredit"),
            "other_description": detail.get("otherDescription"),
        }
    )
    op.upsert("orders", record)

    for line_index, line in enumerate(detail.get("lineItems", []) or []):
        op.upsert(
            "order_line_items",
            {
                "restaurant_unit_id": unit_id,
                "order_id": order_id,
                "line_index": line_index,
                "unit_price": line.get("unitPrice"),
                "vendor_item_code": line.get("vendorItemCode"),
                "quantity": line.get("quantity"),
                "line_price": line.get("linePrice"),
                "vendor_item_name": line.get("vendorItemName"),
                "company_concept_product_id": line.get("companyConceptProductId"),
                "category_id": line.get("categoryId"),
                "packaging_id": line.get("packagingId"),
            },
        )

    for attachment in detail.get("attachments", []) or []:
        op.upsert(
            "order_attachments",
            {
                "restaurant_unit_id": unit_id,
                "order_id": order_id,
                "attachment_id": attachment.get("attachmentId"),
                # NOTE: attachment_url may be a temporary/expiring signed URL - do not
                # assume long-term validity of this value.
                "attachment_url": attachment.get("attachmentUrl"),
            },
        )


def _sync_orders_for_unit(configuration: dict, unit_id, state: dict):
    """Incrementally sync orders (+ detail, line items, attachments) for one restaurant unit.

    Walks forward in date windows (default 30 days) from the last synced
    boundary - stored per-unit in `state["orders_cursor"]` - through today,
    using `created_date` as the conceptual cursor field. Since the /orders
    endpoint only supports date-level (not timestamp-level) filtering via
    `startDate`/`endDate`, the window's end date is used as the resumable
    boundary rather than the max `created_date` seen. Every order in a window
    is paginated via `nextPage`. State is checkpointed after each window
    completes, so an interrupted sync can resume mid-unit without redoing
    already-synced windows.

    Args:
        configuration: connector configuration dict.
        unit_id: the restaurant unit ID to sync orders for.
        state: mutable sync state dict, updated in place.
    """
    cursor_map = state.setdefault("orders_cursor", {})
    last_synced = cursor_map.get(str(unit_id))
    start = _parse_date(last_synced) if last_synced else _parse_date(
        configuration.get("initial_sync_start", "2023-01-01")
    )
    today = datetime.now(timezone.utc).date()

    if start >= today:
        log.info(f"Orders for restaurant unit {unit_id} already up to date through {start.isoformat()}")
        return

    for window_start, window_end in _date_windows(start, today, __ORDER_WINDOW_DAYS):
        params = {
            "restaurantUnitId": unit_id,
            "startDate": window_start.isoformat(),
            "endDate": window_end.isoformat(),
        }
        log.info(f"Syncing orders for unit {unit_id}: {params['startDate']} to {params['endDate']}")
        for order in _paginated_get(configuration, "/orders", params):
            _sync_one_order(configuration, unit_id, order)

        cursor_map[str(unit_id)] = window_end.isoformat()
        state["orders_cursor"] = cursor_map
        op.checkpoint(state)


def _sync_products_for_unit(configuration: dict, unit_id):
    """Fetch and upsert products and their nested category allocations for one unit."""
    for product in _paginated_get(configuration, "/products", {"restaurantUnitId": unit_id}):
        ccpid = product.get("companyConceptProductId")
        op.upsert(
            "products",
            {
                "restaurant_unit_id": unit_id,
                "company_concept_product_id": ccpid,
                "central_product_id": product.get("centralProductId"),
                "product_name": product.get("productName"),
                "latest_price": product.get("latestPrice"),
                "report_by_unit": product.get("reportByUnit"),
                "tax_exempt": product.get("taxExempt"),
                "item_count": product.get("itemCount"),
            },
        )
        for category in product.get("categories", []) or []:
            op.upsert(
                "product_categories",
                {
                    "restaurant_unit_id": unit_id,
                    "company_concept_product_id": ccpid,
                    "category_id": category.get("categoryId"),
                    "percent_allocation": category.get("percentAllocation"),
                },
            )


def _sync_categories_for_unit(configuration: dict, unit_id):
    """Fetch and upsert categories for one restaurant unit."""
    for category in _paginated_get(configuration, "/categories", {"restaurantUnitId": unit_id}):
        op.upsert(
            "categories",
            {
                "restaurant_unit_id": unit_id,
                "category_id": category.get("categoryId"),
                "category_name": category.get("categoryName"),
                "category_type": category.get("categoryType"),
                "accounting_code": category.get("accountingCode"),
            },
        )


def _sync_vendor_item_packaging(configuration: dict, unit_id, vendor_id, vendor_item_code):
    """Fetch and upsert packaging options for one vendor item (no pagination on this endpoint)."""
    body = _get(
        configuration,
        f"/vendors/{vendor_id}/vendorItems/{vendor_item_code}/packaging",
        {"restaurantUnitId": unit_id},
    )
    for package in _extract_list(body):
        op.upsert(
            "vendor_item_packaging",
            {
                "restaurant_unit_id": unit_id,
                "vendor_id": vendor_id,
                "vendor_item_code": vendor_item_code,
                "packaging_id": package.get("packagingId"),
                "packaging_name": package.get("packagingName"),
                "unit": package.get("unit"),
                "quantity": package.get("quantity"),
            },
        )


def _sync_vendor_items_for_vendor(configuration: dict, unit_id, vendor_id):
    """Fetch and upsert vendor_items for one vendor, fanning out to packaging per item."""
    for item in _paginated_get(
        configuration, f"/vendors/{vendor_id}/vendorItems", {"restaurantUnitId": unit_id}
    ):
        vendor_item_code = item.get("vendorItemCode")
        op.upsert(
            "vendor_items",
            {
                "restaurant_unit_id": unit_id,
                "vendor_id": vendor_id,
                "vendor_item_code": vendor_item_code,
                "central_vendor_item_id": item.get("centralVendorItemId"),
                "vendor_name": item.get("vendorName"),
                "central_vendor_id": item.get("centralVendorId"),
                "company_concept_product_id": item.get("companyConceptProductId"),
                "product_name": item.get("productName"),
            },
        )
        if vendor_item_code is not None:
            _sync_vendor_item_packaging(configuration, unit_id, vendor_id, vendor_item_code)


def _sync_vendors_for_unit(configuration: dict, unit_id):
    """Fetch and upsert vendors and their nested accounts for one unit, fanning out to vendor items."""
    for vendor in _paginated_get(configuration, "/vendors", {"restaurantUnitId": unit_id}):
        vendor_id = vendor.get("vendorId")
        op.upsert(
            "vendors",
            {
                "restaurant_unit_id": unit_id,
                "vendor_id": vendor_id,
                "vendor_name": vendor.get("vendorName"),
                "central_vendor_id": vendor.get("centralVendorId"),
            },
        )
        for account_index, account in enumerate(vendor.get("vendorAccounts", []) or []):
            op.upsert(
                "vendor_accounts",
                {
                    "restaurant_unit_id": unit_id,
                    "vendor_id": vendor_id,
                    "account_index": account_index,
                    "vendor_account_number": account.get("vendorAccountNumber"),
                },
            )
        if vendor_id is not None:
            _sync_vendor_items_for_vendor(configuration, unit_id, vendor_id)


def update(configuration: dict, state: dict):
    """Sync all MarginEdge data into the destination.

    Order of operations:
      1. Fetch restaurant_units once (root of every fan-out below) and reuse
         it for every restaurant-scoped table instead of re-fetching.
      2. Sync the two account-level, unit-independent tables (groups, group
         categories).
      3. For each restaurant unit: incrementally sync orders (with detail,
         line items, attachments), then full-refresh products, categories,
         and vendors (which fan out further into vendor_items and
         vendor_item_packaging).

    A 403 response from any call is treated as non-retryable and propagates
    up to abort the sync entirely (invalid key or unit not authorized),
    rather than being silently skipped, per MarginEdge's error semantics.

    Args:
        configuration: dictionary containing connection details (`api_key`,
            `initial_sync_start`).
        state: dictionary containing the state checkpointed during the
            prior sync, or empty for the first sync.
    """
    validate_configuration(configuration)
    log.info("Starting MarginEdge sync")

    units = _sync_restaurant_units(configuration)
    op.checkpoint(state)

    _sync_restaurant_unit_groups(configuration)
    _sync_restaurant_unit_group_categories(configuration)
    op.checkpoint(state)

    for unit in units:
        unit_id = unit.get("id")
        if unit_id is None:
            log.warning(f"Skipping restaurant unit with no id: {unit}")
            continue

        log.info(f"Syncing restaurant unit {unit_id} ({unit.get('name')})")

        # Incremental, windowed, checkpoints internally per window.
        _sync_orders_for_unit(configuration, unit_id, state)

        # Full-refresh tables - re-synced every run since they are not date-filterable.
        _sync_products_for_unit(configuration, unit_id)
        _sync_categories_for_unit(configuration, unit_id)
        _sync_vendors_for_unit(configuration, unit_id)

        op.checkpoint(state)

    log.info("MarginEdge sync complete")


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
