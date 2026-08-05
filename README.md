# MarginEdge Connector Example

## Connector overview

This is a custom [Fivetran connector](https://fivetran.com/docs/connectors/connector-sdk) implementation to extract and sync data from the [MarginEdge Public API](https://api.marginedge.com/public) into a destination warehouse. MarginEdge is a back-of-house restaurant operations platform providing invoicing, inventory, vendor, and product data.

> **Note on API documentation:** This connector was originally built from a third-party OpenAPI mirror of the MarginEdge Public API (the official docs site blocked automated fetch during code generation), then cross-checked against MarginEdge's official Postman collection, which confirmed field names and response shapes for every table. Two details remain **unverified** against live docs: the `orders` date-filter semantics (`createdDate` vs. `invoiceDate`) and the exact `orderStatus` enum values. This connector intentionally omits the `orderStatus` filter (it syncs all orders regardless of status) and treats `createdDate` as the incremental cursor field. Confirm both during local testing with real credentials before relying on this connector in production.

## Requirements

- A MarginEdge Public API key (issued per user; scoped server-side to whichever restaurant units that user can access)
- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)

## Getting started

This folder is already a complete, ready-to-run Connector SDK project (`connector.py`, `configuration.json`, `requirements.txt`) — you do **not** need to run `fivetran init`, which is only for scaffolding a brand-new empty project and would overwrite these files.

1. Install the SDK (Python 3.9+ required — see [supported versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)):
   ```bash
   pip install fivetran-connector-sdk
   ```
2. From inside this directory, populate `configuration.json` with your real MarginEdge API key using the encryption helper (see [Configuration file](#configuration-file) below) — never edit the file by hand or share the key with anyone else.
3. Run the connector locally:
   ```bash
   cd "<connector_dir>"
   fivetran debug
   ```
   This runs a full sync against the real MarginEdge API (there is no sandbox environment) and writes the synced data to a local DuckDB file you can inspect, without needing a live Fivetran destination connection.

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for more detail on `fivetran debug` and other CLI commands.

## Local testing checklist

Please verify the following while running `fivetran debug` and report back what you find — logs and record counts only, **never** the API key itself:

- [ ] Sync completes without unhandled errors (a 403 will abort the whole sync by design — see [Error handling](#error-handling))
- [ ] Every table gets rows, not just `restaurant_units` — an earlier version of this connector had a bug where every list endpoint returned zero rows despite the API responding correctly (wrong assumption about how the response was wrapped); that's now fixed, but worth confirming broadly on your first real run
- [ ] `orders` rows look correct for a known date range — in particular, confirm whether MarginEdge's `startDate`/`endDate` filter (and thus this connector's incremental cursor) should be based on `createdDate` or `invoiceDate`. Compare a few orders' dates against what you see in the MarginEdge UI for that same window
- [ ] Row counts across `restaurant_units`, `products`, `vendors`, `categories` roughly match what you'd expect from the account being tested (confirms the `restaurantUnitId` fan-out is discovering everything it should)
- [ ] Note how long the initial sync takes — the per-product fan-out (`product_units`, `product_price_history`, `vendor_items_by_product`) roughly triples the API call count for large product catalogs, so first-run duration is worth watching on accounts with many products
- [ ] Re-run `fivetran debug` a second time and confirm the `orders` incremental logic doesn't re-sync the entire history (checkpointed state should pick up from where it left off)

## Features

- Discovers restaurant units at runtime (`GET /restaurantUnits`) and fans that list out to every restaurant-scoped endpoint (orders, products, vendors, categories, countsheets, inventories, recipes, profit & loss and sales reports), so it never needs a restaurant unit ID supplied in configuration
- Further fans out from vendors to vendor items, and from vendor items to vendor item packaging; and from products to product units, product price history, and vendor-items-by-product
- Incremental sync of `orders` via monthly date windows and per-restaurant-unit checkpointed state; all other tables are full-refreshed each run since they are not date-filterable
- Deliberately excludes MarginEdge's async `/exports/*` job endpoints (orders/products/vendor-items/usage/recipes/recipeIngredients/recipeCostHistories exports) - submitting an export is a side-effecting POST that creates a job on the client's account rather than a pure read, and the downloaded file's schema (especially for `usage`, which has no direct GET equivalent) is undocumented
- Cursor-based (`nextPage`) pagination on every list endpoint
- Order detail enrichment merged into the same `orders` row, plus nested `lineItems` and `attachments` flattened into child tables
- Graceful handling of authentication and transient errors: retries with capped exponential backoff on 429/5xx, immediate failure on 403, skip-and-log on 404, raise on 400
- A small fixed delay between requests as a defensive courtesy (MarginEdge does not document a numeric rate limit)

## Configuration file

This connector requires the following configuration fields:

| Field | Description |
|---|---|
| `api_key` | MarginEdge Public API key, sent as the `x-api-key` header on every request |
| `initial_sync_start` | `YYYY-MM-DD` date used to seed the first incremental window for the `orders` table when no prior sync state exists |

Example `configuration.json` (placeholder values only):

```json
{
  "api_key": "YOUR_MARGINEDGE_API_KEY_HERE",
  "initial_sync_start": "2023-01-01"
}
```

To populate this file with real values, run the encryption helper script from the connector directory - **do not** paste credentials in chat or edit `configuration.json` by hand:

macOS/Linux:
```bash
cd "<connector_dir>"
python "<plugin>/tools/enter_configuration.py" "configuration.json"
```

Windows PowerShell:
```powershell
cd "<connector_dir>"
python "<plugin>/tools/enter_configuration.py" "configuration.json"
```

The script prompts for each configuration value and encrypts it in place. If the local encryption secret file does not exist yet, the script creates it first.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector has no additional dependencies beyond what Fivetran pre-installs. It only uses `requests`, which is already [pre-installed](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) in the Connector SDK runtime, so `requirements.txt` intentionally declares nothing.

## Data handling

The connector performs the following actions for each key aspect:
- Authentication: sends the configured `api_key` as an `x-api-key` header on every request (no OAuth, no token refresh, no sandbox environment - production only)
- Discovery: fetches `restaurantUnits` once per sync and reuses that list to drive every restaurant-scoped table, instead of re-fetching per table
- Pagination: follows the opaque `nextPage` cursor on every list endpoint until it is absent from the response
- Orders sync loop: walks forward in ~30-day date windows from the last checkpointed date (or `initial_sync_start`) through today, per restaurant unit; each order found is enriched via its detail endpoint and its nested `lineItems`/`attachments` are flattened into child tables
- Static tables (`products`, `vendors`, `vendor_items`, `vendor_item_packaging`, `categories`, `restaurant_unit_groups`, `restaurant_unit_group_categories`, and their children) are not date-filterable and are re-synced in full every run
- Upserts: emits all data via `op.upsert()`
- Checkpointing: checkpoints after restaurant-unit discovery, after account-level tables, after each orders date window (per restaurant unit), and after each restaurant unit's static tables finish

## Error handling

- 400 Bad Request: raised immediately (malformed request parameters are not silently skipped)
- 403 Forbidden: raised immediately and aborts the sync (invalid API key, or the restaurant unit is not authorized for this key) - not retried
- 404 Not Found: logged and the specific record is skipped (e.g., an order detail lookup for an order ID that no longer resolves)
- 429 Too Many Requests / 5xx: retried per-call, honoring the `Retry-After` header when the response includes one, otherwise capped exponential backoff (base 1s, up to 5 attempts, capped at 60s)
- Adaptive request pacing: every 429 also raises the steady-state delay applied before *every* request (starting at ~150ms, capped at 5s), not just retries of the throttled call. Real-world testing showed that a fixed 150ms delay is too aggressive for large product catalogs once the per-product fan-out (`product_units`, `product_price_history`, `vendor_items_by_product`) multiplies call volume - the connector now settles into a slower sustained rate for the rest of that sync run once it detects it's being throttled, instead of immediately reverting to the aggressive default and re-triggering the same 429s on the very next call

Uses Fivetran SDK logging levels (`log.info`, `log.debug`, `log.warning`, `log.error`) for detailed sync visibility.

### Null primary keys in nested list items

Confirmed in live client data: some nested list items have a null natural ID field even though MarginEdge's documented examples always show one populated (e.g. a product's category allocation can include an "unallocated" remainder with no `categoryId`). Since the destination rejects a null primary key outright, every table whose primary key is sourced from a nested list item's own ID field (`product_categories`, `profit_and_loss_report_categories`, `sales_report_categories`, `order_attachments`, `vendor_item_packaging`, `vendor_items`, `vendor_items_by_product`, `countsheet_sections`, `inventory_sections`, `inventory_section_items`) runs that field through `_fallback_id()`, which substitutes a stable `__unassigned_<index>` value (based on position in the source array) whenever the real ID is missing, so the row is still captured instead of crashing the sync.

## Tables created

### Root / discovery
- `restaurant_units` - every other restaurant-scoped table below is driven by this table's `id` values

### Account-level (unit-independent)
- `restaurant_unit_groups`, `restaurant_unit_group_units` (child of groups)
- `restaurant_unit_group_categories`

### Orders (incremental)
- `orders` - list fields plus detail-enrichment fields (`delivery_charges`, `other_charges`, `tax`, `input_tax_credits`, `credit_amount`, `is_credit`, `other_description`) merged into the same row
- `order_line_items` - flattened from order detail `lineItems[]`, synthetic `line_index` primary key component (no natural unique ID in the source)
- `order_attachments` - flattened from order detail `attachments[]`; `attachment_url` may be a temporary/expiring signed URL

### Products
- `products`, `product_categories` (child of products)

### Vendors
- `vendors`, `vendor_accounts` (child of vendors, synthetic `account_index` primary key component)
- `vendor_items` (fanned out per vendor)
- `vendor_item_packaging` (fanned out per vendor item; no pagination on this endpoint)

### Categories
- `categories`

### Product fan-out (per product, on top of `products`/`product_categories` above)
- `product_units` - unit conversions per product; no natural unique field, synthetic `unit_index` primary key component
- `product_price_history` - paginated price history per product; synthetic `price_history_index` primary key component (running index across pages, since `unitName`+`date` can collide across units)
- `vendor_items_by_product` - vendor items linked to a product (keyed by the more granular `vendorItemId`, distinct from `vendorItemCode` used under Vendors)
- `vendor_item_conversions` (child of `vendor_items_by_product`; synthetic `conversion_index` primary key component)

### Countsheets
- `countsheets`
- `countsheet_sections` (fanned out per countsheet via its detail endpoint)

### Inventories
- `inventories`
- `inventory_sections` (fanned out per inventory)
- `inventory_section_items` (fanned out per section)
- `inventory_section_item_product_codes` (child of section items; synthetic `code_index` primary key component)

### Recipes
- `recipe_types`
- `recipes`
- `recipe_ingredients` - fetched once per restaurant unit across all recipes (no per-recipe fan-out)
- `recipe_conversions` - fetched once per restaurant unit across all recipes
- `recipe_cost_histories` - keyed by `(restaurant_unit_id, recipe_id, recorded_date)`, since this resource has no dedicated ID field

### Reports (single-shot per restaurant unit, covering `initial_sync_start` through today)
- `profit_and_loss_reports`, `profit_and_loss_report_categories`, `profit_and_loss_report_category_items` (nested inside each category), `profit_and_loss_report_section_items` (section-level items, sibling to categories - a distinct shape from the category-nested items)
- `sales_reports`, `sales_report_categories`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code structure, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. Field names and response shapes have been cross-checked against MarginEdge's official Postman collection, but test thoroughly against a real MarginEdge account - in particular the `orders` date-filter/cursor field noted above - before deploying to production. For inquiries, please reach out to our Support team.

## Resources

- [Fivetran Connector SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)
- [MarginEdge](https://www.marginedge.com/)
