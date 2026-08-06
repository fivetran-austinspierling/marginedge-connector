"""MarginEdge Public API Connector for Fivetran.

Syncs restaurant, order, product, vendor, and category data from the MarginEdge
Public API (https://api.marginedge.com/public) into a destination warehouse.
MarginEdge is a back-of-house restaurant operations platform (invoicing,
inventory, vendor management).

This connector was originally built from a third-party OpenAPI mirror of the
MarginEdge Public API (MarginEdge's official docs site blocked automated
fetch during code generation), and was later cross-checked against the
official MarginEdge Postman collection. That confirmed:
  - List endpoints wrap their array under an endpoint-specific key (e.g.
    `restaurants`, `orders`, `vendorItems`, `packagings`), not a generic
    `data` key or a bare array. `_extract_list()` picks the first list-typed
    value in the response body to handle this uniformly.
  - The `attachments[]` field names on order detail (`attachmentId` /
    `attachmentUrl`) are correct as originally guessed.

STILL UNVERIFIED - confirm during live testing:
  - Whether `GET /orders` `startDate`/`endDate` filters by `createdDate` vs.
    `invoiceDate` (this connector assumes `createdDate` for both the filter
    semantics and the incremental cursor field).
  - The exact `orderStatus` enum values. This connector intentionally omits
    the `orderStatus` filter entirely and syncs all orders regardless of
    status, so this uncertainty should not affect correctness, only means
    status-based filtering is unavailable.

IMPLEMENTED (previously listed here as a scope gap): the official Postman
collection revealed additional MarginEdge resources beyond the original 14
tables, and these have now been implemented: product units, product price
history, count sheets (+ sections), inventories (+ sections, items, product
codes), recipe types, recipes, recipe ingredients, recipe conversions, recipe
cost histories, profit & loss reports (+ category and section-item child
tables), sales reports (+ category child table), and vendor-items-by-product
(+ conversions child table).

INTENTIONALLY EXCLUDED - the async export-job endpoints under `/exports/*`
(orders, products, vendor-items, usage, recipes, recipeIngredients,
recipeCostHistories) are deliberately NOT implemented. Submitting an export is
a side-effecting POST that creates an async job on the client's MarginEdge
account rather than performing a pure read, and the schema of the resulting
downloaded file - especially for `usage`, which has no direct GET equivalent
at all - is undocumented in the Postman collection. This is a deliberate
scope exclusion, not an oversight.

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
__REQUEST_DELAY = 0.15  # seconds, starting fixed delay between every request
__MAX_REQUEST_DELAY = 5.0  # seconds, cap for the adaptive steady-state delay below
__ORDER_WINDOW_DAYS = 30  # size of each incremental orders date window

# Mutable, module-level (not persisted in sync state - resets every sync run):
# tracks the current steady-state delay applied before every request. Plain
# per-call exponential backoff (below) only slows down retries of the call
# that got throttled; it does nothing for the next, unrelated call, so a
# sustained burst of 429s (e.g. the large per-product fan-out) just keeps
# re-triggering the same throttling. Every 429 nudges this delay up so the
# connector settles into a slower sustained rate instead of immediately
# reverting to the aggressive default.
_PACING = {"delay": __REQUEST_DELAY}


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


def _retry_after_seconds(response) -> float:
    """Return the `Retry-After` header value in seconds if present and valid, else None.

    MarginEdge doesn't document sending this header, but it costs nothing to
    check - if the AWS API Gateway throttle does send it, it's a more
    accurate wait time than a blind exponential guess.
    """
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _get(configuration: dict, path: str, params: dict = None):
    """Issue a single GET request to the MarginEdge API with retry/backoff handling.

    - 429 and 5xx responses are retried, honoring the `Retry-After` header
      when present, otherwise using capped exponential backoff (base 1s, up
      to __MAX_RETRIES retries, capped at __MAX_DELAY seconds).
    - Every 429 also raises the shared `_PACING["delay"]` (see module-level
      comment) so subsequent, unrelated requests slow down too - not just
      retries of the throttled call - since a sustained burst of 429s means
      the steady-state rate itself is too fast, not just this one request.
    - 403 responses raise immediately (invalid key or restaurant unit not
      authorized for this key) - this is not retryable.
    - 400 responses raise immediately (malformed request params) rather than
      being silently skipped.
    - 404 responses return None so the caller can log-and-skip that record.
    - `_PACING["delay"]` is applied before every request as a defensive
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
        time.sleep(_PACING["delay"])
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
            if response.status_code == 429 and _PACING["delay"] < __MAX_REQUEST_DELAY:
                _PACING["delay"] = min(__MAX_REQUEST_DELAY, _PACING["delay"] * 1.5)
                log.info(f"Raised steady-state request pacing to {_PACING['delay']:.2f}s after a 429")

            if attempt >= __MAX_RETRIES:
                log.error(f"Exhausted retries calling {path}: HTTP {response.status_code}")
                response.raise_for_status()

            retry_after = _retry_after_seconds(response)
            delay = retry_after if retry_after is not None else min(__MAX_DELAY, __BASE_DELAY * (2 ** attempt))
            log.warning(
                f"HTTP {response.status_code} calling {path}, retrying in {delay}s "
                f"(attempt {attempt + 1}/{__MAX_RETRIES}"
                f"{', honoring Retry-After' if retry_after is not None else ''})"
            )
            time.sleep(delay)
            attempt += 1
            continue

        # Any other unexpected status code - raise.
        response.raise_for_status()


def _extract_list(body) -> list:
    """Normalize a MarginEdge list-endpoint response body into a list of records.

    Confirmed against both the OpenAPI spec and the official MarginEdge Postman
    collection: every list endpoint wraps its array under an endpoint-specific
    key (e.g. `restaurants`, `orders`, `vendorItems`, `packagings`) rather than
    a generic `data` key or a bare array, and the wrapper key differs per
    endpoint. Every observed response has at most one list-valued top-level
    key (aside from the `nextPage` cursor string), so picking the first
    list-typed value works uniformly across all endpoints without needing a
    per-endpoint key map.
    """
    if body is None:
        return []
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for value in body.values():
            if isinstance(value, list):
                return value
        return []
    return []


def _is_blank(value) -> bool:
    """True for None or an empty/whitespace-only string.

    Confirmed in live client data: MarginEdge can send an empty string
    (not just JSON null) where an ID field is missing. Treating only `None`
    as missing let a blank `vendorItemCode` pass an `is not None` fan-out
    guard unnoticed and get spliced directly into a URL path, producing a
    malformed double-slash path (".../vendorItems//packaging") and an
    unhandled 400. Every check for "is this ID actually present" - whether
    feeding a primary key or building a request URL - must treat blank the
    same as null.
    """
    return value is None or (isinstance(value, str) and value.strip() == "")


def _fallback_id(value, index):
    """Return `value` if present, else a stable synthetic ID derived from position.

    Confirmed in live client data: some MarginEdge nested list items (e.g. a
    product's category allocations, which can include an "unallocated"
    remainder with no category) have a missing natural ID field (null or
    blank string), even though the API's documented examples always show
    one populated. A null primary key value is rejected outright by the
    destination, so every PK field sourced from a nested list item's own ID
    field runs through this rather than assuming the field is always
    present.
    """
    return value if not _is_blank(value) else f"__unassigned_{index}"


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
        {
            "table": "product_units",
            "primary_key": ["restaurant_unit_id", "company_concept_product_id", "unit_index"],
        },
        {
            "table": "product_price_history",
            "primary_key": ["restaurant_unit_id", "company_concept_product_id", "price_history_index"],
        },
        {
            "table": "vendor_items_by_product",
            "primary_key": ["restaurant_unit_id", "company_concept_product_id", "vendor_item_id"],
        },
        {
            "table": "vendor_item_conversions",
            "primary_key": [
                "restaurant_unit_id",
                "company_concept_product_id",
                "vendor_item_id",
                "conversion_index",
            ],
        },
        {"table": "countsheets", "primary_key": ["restaurant_unit_id", "countsheet_id"]},
        {
            "table": "countsheet_sections",
            "primary_key": ["restaurant_unit_id", "countsheet_id", "section_id"],
        },
        {"table": "inventories", "primary_key": ["restaurant_unit_id", "inventory_id"]},
        {
            "table": "inventory_sections",
            "primary_key": ["restaurant_unit_id", "inventory_id", "section_id"],
        },
        {
            "table": "inventory_section_items",
            "primary_key": ["restaurant_unit_id", "inventory_id", "section_id", "item_id"],
        },
        {
            "table": "inventory_section_item_product_codes",
            "primary_key": [
                "restaurant_unit_id",
                "inventory_id",
                "section_id",
                "item_id",
                "code_index",
            ],
        },
        {"table": "recipe_types", "primary_key": ["restaurant_unit_id", "recipe_type_id"]},
        {"table": "recipes", "primary_key": ["restaurant_unit_id", "recipe_id"]},
        {"table": "recipe_ingredients", "primary_key": ["restaurant_unit_id", "ingredient_id"]},
        {
            "table": "recipe_conversions",
            "primary_key": ["restaurant_unit_id", "recipe_conversion_id"],
        },
        {
            "table": "recipe_cost_histories",
            "primary_key": ["restaurant_unit_id", "recipe_id", "recorded_date"],
        },
        {
            "table": "profit_and_loss_reports",
            "primary_key": ["restaurant_unit_id", "start_date", "end_date"],
        },
        {
            "table": "profit_and_loss_report_categories",
            "primary_key": ["restaurant_unit_id", "start_date", "end_date", "section", "category_id"],
        },
        {
            "table": "profit_and_loss_report_category_items",
            "primary_key": [
                "restaurant_unit_id",
                "start_date",
                "end_date",
                "section",
                "category_id",
                "item_index",
            ],
        },
        {
            "table": "profit_and_loss_report_section_items",
            "primary_key": ["restaurant_unit_id", "start_date", "end_date", "section", "item_index"],
        },
        {"table": "sales_reports", "primary_key": ["restaurant_unit_id", "start_date", "end_date"]},
        {
            "table": "sales_report_categories",
            "primary_key": ["restaurant_unit_id", "start_date", "end_date", "category_id"],
        },
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
    if _is_blank(order_id):
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

    for attachment_index, attachment in enumerate(detail.get("attachments", []) or []):
        op.upsert(
            "order_attachments",
            {
                "restaurant_unit_id": unit_id,
                "order_id": order_id,
                "attachment_id": _fallback_id(attachment.get("attachmentId"), attachment_index),
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


def _sync_product_units(configuration: dict, unit_id, ccpid):
    """Fetch and upsert unit conversions for one product (no pagination on this endpoint).

    Args:
        configuration: connector configuration dict.
        unit_id: the restaurant unit ID this product belongs to.
        ccpid: the product's companyConceptProductId.
    """
    body = _get(configuration, f"/products/{ccpid}/units", {"restaurantUnitId": unit_id})
    for unit_index, unit in enumerate(_extract_list(body)):
        op.upsert(
            "product_units",
            {
                "restaurant_unit_id": unit_id,
                "company_concept_product_id": ccpid,
                "unit_index": unit_index,
                "packaging_name": unit.get("packagingName"),
                "unit": unit.get("unit"),
                "quantity": unit.get("quantity"),
                "price": unit.get("price"),
                "ratio": unit.get("ratio"),
                "ratio_display": unit.get("ratioDisplay"),
                "parent_unit": unit.get("parentUnit"),
                "is_count_by_unit": unit.get("isCountByUnit"),
                "is_report_by_unit": unit.get("isReportByUnit"),
                "is_used_on_inventory": unit.get("isUsedOnInventory"),
            },
        )


def _sync_product_price_history(configuration: dict, unit_id, ccpid):
    """Fetch and upsert price history for one product, paginated via `nextPage`.

    No natural unique field exists per entry (unitName + date could collide
    across different units on the same date), so a 0-based running index
    across all pages is used as part of the primary key.

    Args:
        configuration: connector configuration dict.
        unit_id: the restaurant unit ID this product belongs to.
        ccpid: the product's companyConceptProductId.
    """
    for price_history_index, entry in enumerate(
        _paginated_get(configuration, f"/products/{ccpid}/priceHistory", {"restaurantUnitId": unit_id})
    ):
        op.upsert(
            "product_price_history",
            {
                "restaurant_unit_id": unit_id,
                "company_concept_product_id": ccpid,
                "price_history_index": price_history_index,
                "unit_name": entry.get("unitName"),
                "date": entry.get("date"),
                "price": entry.get("price"),
            },
        )


def _sync_vendor_items_by_product(configuration: dict, unit_id, ccpid):
    """Fetch and upsert vendor items linked to one product, fanning out to conversions.

    NOTE: `vendorItemId` here is a different, more granular field than the
    `vendorItemCode` used by `_sync_vendor_items_for_vendor` elsewhere in this
    connector - the two are not interchangeable.

    Args:
        configuration: connector configuration dict.
        unit_id: the restaurant unit ID this product belongs to.
        ccpid: the product's companyConceptProductId.
    """
    for item_index, item in enumerate(_paginated_get(
        configuration,
        "/vendorItemsByProduct",
        {"restaurantUnitId": unit_id, "companyConceptProductId": ccpid},
    )):
        vendor_item_id = _fallback_id(item.get("vendorItemId"), item_index)
        op.upsert(
            "vendor_items_by_product",
            {
                "restaurant_unit_id": unit_id,
                "company_concept_product_id": ccpid,
                "vendor_item_id": vendor_item_id,
                "vendor_id": item.get("vendorId"),
                "central_vendor_id": item.get("centralVendorId"),
                "vendor_name": item.get("vendorName"),
                "vendor_item_code": item.get("vendorItemCode"),
                "central_vendor_item_id": item.get("centralVendorItemId"),
                "vendor_item_name": item.get("vendorItemName"),
            },
        )
        for conversion_index, conversion in enumerate(item.get("vendorItemConversions", []) or []):
            op.upsert(
                "vendor_item_conversions",
                {
                    "restaurant_unit_id": unit_id,
                    "company_concept_product_id": ccpid,
                    "vendor_item_id": vendor_item_id,
                    "conversion_index": conversion_index,
                    "vendor_product_unit_id": conversion.get("vendorProductUnitId"),
                    "packaging": conversion.get("packaging"),
                    "unit": conversion.get("unit"),
                    "quantity": conversion.get("quantity"),
                    "price": conversion.get("price"),
                    "conversion_ratio": conversion.get("conversionRatio"),
                    "last_ordered_date": conversion.get("lastOrderedDate"),
                    "order_guide": conversion.get("orderGuide"),
                },
            )


def _sync_products_for_unit(configuration: dict, unit_id):
    """Fetch and upsert products and their nested category allocations for one unit,
    then fan out per product to product_units, product_price_history, and
    vendor_items_by_product (+ its vendor_item_conversions child table).

    The product list is materialized up front (rather than streamed directly
    from `_paginated_get`) purely so the per-product fan-out count can be
    logged before the slower fan-out loop begins.
    """
    products = list(_paginated_get(configuration, "/products", {"restaurantUnitId": unit_id}))
    log.info(
        f"Starting per-product fan-out (units, price history, vendor items) for "
        f"{len(products)} products in restaurant unit {unit_id}; this roughly "
        f"triples the API call count for this unit's product catalog and may be "
        f"slow on large initial syncs"
    )
    for product_index, product in enumerate(products):
        raw_ccpid = product.get("companyConceptProductId")
        ccpid = _fallback_id(raw_ccpid, product_index)
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
        for category_index, category in enumerate(product.get("categories", []) or []):
            op.upsert(
                "product_categories",
                {
                    "restaurant_unit_id": unit_id,
                    "company_concept_product_id": ccpid,
                    "category_id": _fallback_id(category.get("categoryId"), category_index),
                    "percent_allocation": category.get("percentAllocation"),
                },
            )

        if not _is_blank(raw_ccpid):
            _sync_product_units(configuration, unit_id, ccpid)
            _sync_product_price_history(configuration, unit_id, ccpid)
            _sync_vendor_items_by_product(configuration, unit_id, ccpid)


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
    for package_index, package in enumerate(_extract_list(body)):
        op.upsert(
            "vendor_item_packaging",
            {
                "restaurant_unit_id": unit_id,
                "vendor_id": vendor_id,
                "vendor_item_code": vendor_item_code,
                "packaging_id": _fallback_id(package.get("packagingId"), package_index),
                "packaging_name": package.get("packagingName"),
                "unit": package.get("unit"),
                "quantity": package.get("quantity"),
            },
        )


def _sync_vendor_items_for_vendor(configuration: dict, unit_id, vendor_id):
    """Fetch and upsert vendor_items for one vendor, fanning out to packaging per item."""
    for item_index, item in enumerate(_paginated_get(
        configuration, f"/vendors/{vendor_id}/vendorItems", {"restaurantUnitId": unit_id}
    )):
        raw_vendor_item_code = item.get("vendorItemCode")
        vendor_item_code = _fallback_id(raw_vendor_item_code, item_index)
        op.upsert(
            "vendor_items",
            {
                "restaurant_unit_id": unit_id,
                "vendor_id": vendor_id,
                "vendor_item_code": vendor_item_code,
                "vendor_item_id": item.get("vendorItemId"),
                "vendor_item_name": item.get("vendorItemName"),
                "central_vendor_item_id": item.get("centralVendorItemId"),
                "vendor_name": item.get("vendorName"),
                "central_vendor_id": item.get("centralVendorId"),
                "company_concept_product_id": item.get("companyConceptProductId"),
                "product_name": item.get("productName"),
            },
        )
        if not _is_blank(raw_vendor_item_code):
            _sync_vendor_item_packaging(configuration, unit_id, vendor_id, vendor_item_code)


def _sync_vendors_for_unit(configuration: dict, unit_id):
    """Fetch and upsert vendors and their nested accounts for one unit, fanning out to vendor items."""
    for vendor_index, vendor in enumerate(
        _paginated_get(configuration, "/vendors", {"restaurantUnitId": unit_id})
    ):
        raw_vendor_id = vendor.get("vendorId")
        vendor_id = _fallback_id(raw_vendor_id, vendor_index)
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
        if not _is_blank(raw_vendor_id):
            _sync_vendor_items_for_vendor(configuration, unit_id, vendor_id)


def _sync_countsheet_sections(configuration: dict, unit_id, countsheet_id):
    """Fetch and upsert sections for one countsheet (detail endpoint, no pagination).

    The detail response repeats the countsheet's list-level fields plus adds
    `sections[]`; only the sections are used here since the parent row was
    already upserted from the list call.
    """
    detail = _get(configuration, f"/countsheets/{countsheet_id}", {"restaurantUnitId": unit_id})
    if detail is None:
        return
    for section_index, section in enumerate(detail.get("sections", []) or []):
        op.upsert(
            "countsheet_sections",
            {
                "restaurant_unit_id": unit_id,
                "countsheet_id": countsheet_id,
                "section_id": _fallback_id(section.get("sectionId"), section_index),
                "name": section.get("name"),
                "position": section.get("position"),
                "category_id": section.get("categoryId"),
                "category_name": section.get("categoryName"),
                "item_count": section.get("itemCount"),
            },
        )


def _sync_countsheets_for_unit(configuration: dict, unit_id):
    """Fetch and upsert countsheets and their nested sections for one restaurant unit."""
    for countsheet_index, countsheet in enumerate(
        _paginated_get(configuration, "/countsheets", {"restaurantUnitId": unit_id})
    ):
        raw_countsheet_id = countsheet.get("countsheetId")
        countsheet_id = _fallback_id(raw_countsheet_id, countsheet_index)
        op.upsert(
            "countsheets",
            {
                "restaurant_unit_id": unit_id,
                "countsheet_id": countsheet_id,
                "name": countsheet.get("name"),
                "disabled": countsheet.get("disabled"),
                "origin": countsheet.get("origin"),
                "last_count_date": countsheet.get("lastCountDate"),
                "remote_id": countsheet.get("remoteId"),
                "last_modified_date": countsheet.get("lastModifiedDate"),
            },
        )
        if not _is_blank(raw_countsheet_id):
            _sync_countsheet_sections(configuration, unit_id, countsheet_id)


def _sync_inventory_section_items(configuration: dict, unit_id, inventory_id, section_id):
    """Fetch and upsert items within one inventory section, fanning out to product codes."""
    for item_index, item in enumerate(_paginated_get(
        configuration,
        f"/inventories/{inventory_id}/sections/{section_id}/items",
        {"restaurantUnitId": unit_id},
    )):
        item_id = _fallback_id(item.get("itemId"), item_index)
        op.upsert(
            "inventory_section_items",
            {
                "restaurant_unit_id": unit_id,
                "inventory_id": inventory_id,
                "section_id": section_id,
                "item_id": item_id,
                "position": item.get("position"),
                "product_id": item.get("productId"),
                "product_name": item.get("productName"),
                "company_concept_product_id": item.get("companyConceptProductId"),
                "central_product_id": item.get("centralProductId"),
                "quantity": item.get("quantity"),
                "price": item.get("price"),
                "value": item.get("value"),
                "unit": item.get("unit"),
                "unit_size": item.get("unitSize"),
            },
        )
        for code_index, product_code in enumerate(item.get("productCodes", []) or []):
            op.upsert(
                "inventory_section_item_product_codes",
                {
                    "restaurant_unit_id": unit_id,
                    "inventory_id": inventory_id,
                    "section_id": section_id,
                    "item_id": item_id,
                    "code_index": code_index,
                    "product_code": product_code,
                },
            )


def _sync_inventory_sections(configuration: dict, unit_id, inventory_id):
    """Fetch and upsert sections for one inventory, fanning out to section items.

    A single inventory's `sections[]` is a small finite list, but the
    `nextPage` cursor is still followed defensively via `_paginated_get`,
    consistent with how this connector treats every other list endpoint.
    """
    for section_index, section in enumerate(_paginated_get(
        configuration, f"/inventories/{inventory_id}", {"restaurantUnitId": unit_id}
    )):
        raw_section_id = section.get("sectionId")
        section_id = _fallback_id(raw_section_id, section_index)
        op.upsert(
            "inventory_sections",
            {
                "restaurant_unit_id": unit_id,
                "inventory_id": inventory_id,
                "section_id": section_id,
                "name": section.get("name"),
                "position": section.get("position"),
            },
        )
        if not _is_blank(raw_section_id):
            _sync_inventory_section_items(configuration, unit_id, inventory_id, section_id)


def _sync_inventories_for_unit(configuration: dict, unit_id):
    """Fetch and upsert inventories for one restaurant unit, fanning out to their
    sections and section items (which further fan out to product codes)."""
    for inventory_index, inventory in enumerate(
        _paginated_get(configuration, "/inventories", {"restaurantUnitId": unit_id})
    ):
        raw_inventory_id = inventory.get("inventoryId")
        inventory_id = _fallback_id(raw_inventory_id, inventory_index)
        op.upsert(
            "inventories",
            {
                "restaurant_unit_id": unit_id,
                "inventory_id": inventory_id,
                "countsheet_id": inventory.get("countsheetId"),
                "countsheet_name": inventory.get("countsheetName"),
                "inventory_date": inventory.get("inventoryDate"),
                "status": inventory.get("status"),
                "total_value": inventory.get("totalValue"),
                "closed_date": inventory.get("closedDate"),
                "first_closed_date": inventory.get("firstClosedDate"),
                "saved_date": inventory.get("savedDate"),
                "origin": inventory.get("origin"),
            },
        )
        if not _is_blank(raw_inventory_id):
            _sync_inventory_sections(configuration, unit_id, inventory_id)


def _sync_recipe_types_for_unit(configuration: dict, unit_id):
    """Fetch and upsert recipe types for one restaurant unit (no pagination on this endpoint)."""
    body = _get(configuration, "/recipeTypes", {"restaurantUnitId": unit_id})
    for recipe_type in _extract_list(body):
        op.upsert(
            "recipe_types",
            {
                "restaurant_unit_id": unit_id,
                "recipe_type_id": recipe_type.get("recipeTypeId"),
                "recipe_type_name": recipe_type.get("recipeTypeName"),
                "recipe_category_type": recipe_type.get("recipeCategoryType"),
                "created_date": recipe_type.get("createdDate"),
                "concept_recipe_type_id": recipe_type.get("conceptRecipeTypeId"),
            },
        )


def _sync_recipes_for_unit(configuration: dict, unit_id):
    """Fetch and upsert all recipes for one restaurant unit (no `recipeId` filter,
    so this fetches every recipe for the unit in one paginated sweep)."""
    for recipe in _paginated_get(configuration, "/recipes", {"restaurantUnitId": unit_id}):
        op.upsert(
            "recipes",
            {
                "restaurant_unit_id": unit_id,
                "recipe_id": recipe.get("recipeId"),
                "recipe_name": recipe.get("recipeName"),
                "recipe_cost": recipe.get("recipeCost"),
                "last_recipe_cost_update": recipe.get("lastRecipeCostUpdate"),
                "yield_quantity": recipe.get("yieldQuantity"),
                "unit": recipe.get("unit"),
                "menu_price": recipe.get("menuPrice"),
                "recipe_type_id": recipe.get("recipeTypeId"),
                "recipe_type_name": recipe.get("recipeTypeName"),
                "recipe_category_type": recipe.get("recipeCategoryType"),
                "on_inventory": recipe.get("onInventory"),
                "is_inactive": recipe.get("isInactive"),
                "created_date": recipe.get("createdDate"),
                "last_modified_date": recipe.get("lastModifiedDate"),
                "report_by_quantity": recipe.get("reportByQuantity"),
                "report_by_ratio": recipe.get("reportByRatio"),
                "report_by_unit": recipe.get("reportByUnit"),
                "report_by_conversion_quantity": recipe.get("reportByConversionQuantity"),
                "lock_inventory": recipe.get("lockInventory"),
                "shelf_life_days": recipe.get("shelfLifeDays"),
                "is_location_restricted": recipe.get("isLocationRestricted"),
                "plate_cost_percentage": recipe.get("plateCostPercentage"),
                "commissary_conversion": recipe.get("commissaryConversion"),
                "commissary": recipe.get("commissary"),
                "commissary_vendor_item_id": recipe.get("commissaryVendorItemId"),
                "commissary_yield_id": recipe.get("commissaryYieldId"),
                "commissary_use_ingredient_categories": recipe.get(
                    "commissaryUseIngredientCategories"
                ),
                "has_unmatched_ingredients": recipe.get("hasUnmatchedIngredients"),
                "uses_location_specific_menu_pricing": recipe.get(
                    "usesLocationSpecificMenuPricing"
                ),
                "equipment": recipe.get("equipment"),
                "lower_plate_cost_percentage_alert_bound": recipe.get(
                    "lowerPlateCostPercentageAlertBound"
                ),
                "upper_plate_cost_percentage_alert_bound": recipe.get(
                    "upperPlateCostPercentageAlertBound"
                ),
                "is_plate_cost_out_of_bounds": recipe.get("isPlateCostOutOfBounds"),
            },
        )


def _sync_recipe_ingredients_for_unit(configuration: dict, unit_id):
    """Fetch and upsert all recipe ingredients for one restaurant unit.

    The `recipeId` filter is intentionally omitted so this sweeps every
    ingredient of every recipe for the unit in a single paginated call,
    rather than fanning out per recipe.
    """
    for ingredient in _paginated_get(
        configuration, "/recipeIngredients", {"restaurantUnitId": unit_id}
    ):
        op.upsert(
            "recipe_ingredients",
            {
                "restaurant_unit_id": unit_id,
                "ingredient_id": ingredient.get("ingredientId"),
                "recipe_id": ingredient.get("recipeId"),
                "recipe_name": ingredient.get("recipeName"),
                "ingredient_position": ingredient.get("ingredientPosition"),
                "ingredient_name": ingredient.get("ingredientName"),
                "ingredient_cost": ingredient.get("ingredientCost"),
                "quantity": ingredient.get("quantity"),
                "unit": ingredient.get("unit"),
                "yield_percentage": ingredient.get("yieldPercentage"),
                "company_concept_product_id": ingredient.get("companyConceptProductId"),
                "sub_recipe_id": ingredient.get("subRecipeId"),
                "ingredient_type": ingredient.get("ingredientType"),
                "notes": ingredient.get("notes"),
                "created_date": ingredient.get("createdDate"),
                "product_report_by_unit": ingredient.get("productReportByUnit"),
            },
        )


def _sync_recipe_conversions_for_unit(configuration: dict, unit_id):
    """Fetch and upsert all recipe conversions for one restaurant unit (no `recipeId`
    filter, so this fetches every conversion for the unit in one paginated sweep)."""
    for conversion in _paginated_get(
        configuration, "/recipeConversions", {"restaurantUnitId": unit_id}
    ):
        op.upsert(
            "recipe_conversions",
            {
                "restaurant_unit_id": unit_id,
                "recipe_conversion_id": conversion.get("recipeConversionId"),
                "recipe_id": conversion.get("recipeId"),
                "recipe_name": conversion.get("recipeName"),
                "quantity": conversion.get("quantity"),
                "unit": conversion.get("unit"),
                "note": conversion.get("note"),
                "created_date": conversion.get("createdDate"),
            },
        )


def _sync_recipe_cost_histories_for_unit(configuration: dict, unit_id):
    """Fetch and upsert recipe cost history entries for one restaurant unit.

    No dedicated ID field exists on this resource, so the (recipe_id,
    recorded_date) pair is used as the natural key.
    """
    for entry in _paginated_get(
        configuration, "/recipeCostHistories", {"restaurantUnitId": unit_id}
    ):
        op.upsert(
            "recipe_cost_histories",
            {
                "restaurant_unit_id": unit_id,
                "recipe_id": entry.get("recipeId"),
                "recorded_date": entry.get("recordedDate"),
                "recipe_name": entry.get("recipeName"),
                "recipe_cost": entry.get("recipeCost"),
            },
        )


def _sync_profit_and_loss_section(
    configuration: dict, unit_id, start_date, end_date, section_name, section
):
    """Upsert one section (income/cogs/expenses/labor) of a profit & loss report.

    Handles both `items[]` nested inside each category and the section-level
    `items[]` that are siblings of `categories[]` (not nested in any
    category) - these are two distinct shapes per the API response.

    Args:
        configuration: connector configuration dict (unused, kept for signature
            consistency with other `_sync_*` helpers).
        unit_id: the restaurant unit ID this report belongs to.
        start_date: the report's start date, part of the composite key.
        end_date: the report's end date, part of the composite key.
        section_name: one of "income", "cogs", "expenses", "labor".
        section: the raw section dict from the report response.
    """
    if not section:
        return
    for category_index, category in enumerate(section.get("categories", []) or []):
        category_id = _fallback_id(category.get("id"), category_index)
        op.upsert(
            "profit_and_loss_report_categories",
            {
                "restaurant_unit_id": unit_id,
                "start_date": start_date,
                "end_date": end_date,
                "section": section_name,
                "category_id": category_id,
                "category_name": category.get("name"),
                "total": category.get("total"),
                "percent_of_sales": category.get("percentOfSales"),
            },
        )
        for item_index, item in enumerate(category.get("items", []) or []):
            op.upsert(
                "profit_and_loss_report_category_items",
                {
                    "restaurant_unit_id": unit_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "section": section_name,
                    "category_id": category_id,
                    "item_index": item_index,
                    "name": item.get("name"),
                    "total": item.get("total"),
                    "percent_of_sales": item.get("percentOfSales"),
                },
            )

    for item_index, item in enumerate(section.get("items", []) or []):
        op.upsert(
            "profit_and_loss_report_section_items",
            {
                "restaurant_unit_id": unit_id,
                "start_date": start_date,
                "end_date": end_date,
                "section": section_name,
                "item_index": item_index,
                "name": item.get("name"),
                "total": item.get("total"),
                "percent_of_sales": item.get("percentOfSales"),
            },
        )


def _sync_profit_and_loss_for_unit(configuration: dict, unit_id):
    """Fetch and upsert the profit & loss report for one restaurant unit.

    This is a single-shot call (no pagination) covering the full configured
    date range - `configuration.get("initial_sync_start", "2023-01-01")`
    through today, the same default already used for orders - rather than
    windowed per-run like orders. The response body is a list even though
    scoped to a single restaurantUnitId (should be 0 or 1 entries for a
    single-unit request), so it is iterated defensively rather than assuming
    exactly one entry.
    """
    start = configuration.get("initial_sync_start", "2023-01-01")
    end = datetime.now(timezone.utc).date().isoformat()
    body = _get(
        configuration,
        "/profitAndLoss/report",
        {"restaurantUnitId": unit_id, "startDate": start, "endDate": end},
    )
    for report in _extract_list(body):
        report_start = report.get("startDate")
        report_end = report.get("endDate")
        summary = report.get("summary", {}) or {}
        income = report.get("income", {}) or {}
        cogs = report.get("cogs", {}) or {}
        expenses = report.get("expenses", {}) or {}
        labor = report.get("labor", {}) or {}

        op.upsert(
            "profit_and_loss_reports",
            {
                "restaurant_unit_id": unit_id,
                "start_date": report_start,
                "end_date": report_end,
                "restaurant_unit_name": report.get("restaurantUnitName"),
                "company_id": report.get("companyId"),
                "company_name": report.get("companyName"),
                "concept_id": report.get("conceptId"),
                "concept_name": report.get("conceptName"),
                "currency": report.get("currency"),
                "summary_gross_profit": summary.get("grossProfit"),
                "summary_gross_profit_percent_of_sales": summary.get(
                    "grossProfitPercentOfSales"
                ),
                "summary_prime_cost_total": summary.get("primeCostTotal"),
                "summary_prime_cost_percent_of_sales": summary.get(
                    "primeCostPercentOfSales"
                ),
                "summary_controllable_profit": summary.get("controllableProfit"),
                "summary_controllable_profit_percent_of_sales": summary.get(
                    "controllableProfitPercentOfSales"
                ),
                "income_total": income.get("total"),
                "income_total_percent_of_sales": income.get("totalPercentOfSales"),
                "cogs_total": cogs.get("total"),
                "cogs_total_percent_of_sales": cogs.get("totalPercentOfSales"),
                "expenses_total": expenses.get("total"),
                "expenses_total_percent_of_sales": expenses.get("totalPercentOfSales"),
                "labor_total": labor.get("total"),
                "labor_total_percent_of_sales": labor.get("totalPercentOfSales"),
            },
        )

        _sync_profit_and_loss_section(
            configuration, unit_id, report_start, report_end, "income", income
        )
        _sync_profit_and_loss_section(
            configuration, unit_id, report_start, report_end, "cogs", cogs
        )
        _sync_profit_and_loss_section(
            configuration, unit_id, report_start, report_end, "expenses", expenses
        )
        _sync_profit_and_loss_section(
            configuration, unit_id, report_start, report_end, "labor", labor
        )


def _sync_sales_report_for_unit(configuration: dict, unit_id):
    """Fetch and upsert the sales report for one restaurant unit, fanning out to categories.

    Single-shot call (no pagination) covering the same full configured date
    range as `_sync_profit_and_loss_for_unit`. The response body is a list
    even though scoped to a single restaurantUnitId, so it is iterated
    defensively rather than assuming exactly one entry.
    """
    start = configuration.get("initial_sync_start", "2023-01-01")
    end = datetime.now(timezone.utc).date().isoformat()
    body = _get(
        configuration,
        "/sales/report",
        {"restaurantUnitId": unit_id, "startDate": start, "endDate": end},
    )
    for report in _extract_list(body):
        report_start = report.get("startDate")
        report_end = report.get("endDate")
        summary = report.get("summary", {}) or {}

        op.upsert(
            "sales_reports",
            {
                "restaurant_unit_id": unit_id,
                "start_date": report_start,
                "end_date": report_end,
                "restaurant_unit_name": report.get("restaurantUnitName"),
                "company_id": report.get("companyId"),
                "company_name": report.get("companyName"),
                "concept_id": report.get("conceptId"),
                "concept_name": report.get("conceptName"),
                "currency": report.get("currency"),
                "summary_total_sales": summary.get("totalSales"),
            },
        )
        for category_index, category in enumerate(report.get("categories", []) or []):
            op.upsert(
                "sales_report_categories",
                {
                    "restaurant_unit_id": unit_id,
                    "start_date": report_start,
                    "end_date": report_end,
                    "category_id": _fallback_id(category.get("id"), category_index),
                    "category_name": category.get("name"),
                    "total": category.get("total"),
                    "percent_of_total_sales": category.get("percentOfTotalSales"),
                },
            )


def update(configuration: dict, state: dict):
    """Sync all MarginEdge data into the destination.

    Order of operations:
      1. Fetch restaurant_units once (root of every fan-out below) and reuse
         it for every restaurant-scoped table instead of re-fetching.
      2. Sync the two account-level, unit-independent tables (groups, group
         categories).
      3. For each restaurant unit: incrementally sync orders (with detail,
         line items, attachments), then full-refresh products (which fan out
         further into product_units, product_price_history, and
         vendor_items_by_product), categories, vendors (which fan out further
         into vendor_items and vendor_item_packaging), countsheets (+
         sections), inventories (+ sections, items, product codes), recipe
         types, recipes, recipe ingredients, recipe conversions, recipe cost
         histories, the profit & loss report, and the sales report.

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
        if _is_blank(unit_id):
            log.warning(f"Skipping restaurant unit with no id: {unit}")
            continue

        log.info(f"Syncing restaurant unit {unit_id} ({unit.get('name')})")

        # Incremental, windowed, checkpoints internally per window.
        _sync_orders_for_unit(configuration, unit_id, state)

        # Full-refresh tables - re-synced every run since they are not date-filterable.
        _sync_products_for_unit(configuration, unit_id)
        _sync_categories_for_unit(configuration, unit_id)
        _sync_vendors_for_unit(configuration, unit_id)
        _sync_countsheets_for_unit(configuration, unit_id)
        _sync_inventories_for_unit(configuration, unit_id)
        _sync_recipe_types_for_unit(configuration, unit_id)
        _sync_recipes_for_unit(configuration, unit_id)
        _sync_recipe_ingredients_for_unit(configuration, unit_id)
        _sync_recipe_conversions_for_unit(configuration, unit_id)
        _sync_recipe_cost_histories_for_unit(configuration, unit_id)
        _sync_profit_and_loss_for_unit(configuration, unit_id)
        _sync_sales_report_for_unit(configuration, unit_id)

        op.checkpoint(state)

    log.info("MarginEdge sync complete")


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
