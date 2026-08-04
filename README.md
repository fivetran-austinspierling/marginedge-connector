# MarginEdge Connector Example

## Connector overview

This is a custom [Fivetran connector](https://fivetran.com/docs/connectors/connector-sdk) implementation to extract and sync data from the [MarginEdge Public API](https://api.marginedge.com/public) into a destination warehouse. MarginEdge is a back-of-house restaurant operations platform providing invoicing, inventory, vendor, and product data.

> **Note on API documentation:** This connector was built from a third-party OpenAPI mirror of the MarginEdge Public API, because the official MarginEdge documentation site blocked automated fetch during code generation. In particular, the `orders` date-filter semantics (`createdDate` vs. `invoiceDate`) and the exact `orderStatus` enum values are **unverified** against live docs. This connector intentionally omits the `orderStatus` filter (it syncs all orders regardless of status) and treats `createdDate` as the incremental cursor field. Confirm both details against a live MarginEdge account during local testing with real credentials before relying on this connector in production.

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

Because this connector was built from a third-party API mirror rather than MarginEdge's official docs (see the note at the top of this file), please verify the following while running `fivetran debug` and report back what you find — logs and record counts only, **never** the API key itself:

- [ ] Sync completes without unhandled errors (a 403 will abort the whole sync by design — see [Error handling](#error-handling))
- [ ] `orders` rows look correct for a known date range — in particular, confirm whether MarginEdge's `startDate`/`endDate` filter (and thus this connector's incremental cursor) should be based on `createdDate` or `invoiceDate`. Compare a few orders' dates against what you see in the MarginEdge UI for that same window.
- [ ] `order_attachments.attachment_url` and `attachment_id` are actually populated with the field names this connector expects (`attachmentUrl`/`attachmentId`) — if they come back empty/null on orders you know have attachments, the real field names differ and the code needs a small update.
- [ ] Row counts across `restaurant_units`, `products`, `vendors`, `categories` roughly match what you'd expect from the account being tested (confirms the `restaurantUnitId` fan-out is discovering everything it should).
- [ ] Re-run `fivetran debug` a second time and confirm the `orders` incremental logic doesn't re-sync the entire history (checkpointed state should pick up from where it left off).

## Features

- Discovers restaurant units at runtime (`GET /restaurantUnits`) and fans that list out to every restaurant-scoped endpoint (orders, products, vendors, categories), so it never needs a restaurant unit ID supplied in configuration
- Further fans out from vendors to vendor items, and from vendor items to vendor item packaging
- Incremental sync of `orders` via monthly date windows and per-restaurant-unit checkpointed state; all other tables are full-refreshed each run since they are not date-filterable
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
- 429 Too Many Requests / 5xx: retried with capped exponential backoff (base 1s, up to 5 attempts, capped at 60s), since MarginEdge does not document a `Retry-After` header
- A fixed ~150ms delay is added before every request as a defensive courtesy against undocumented rate limits

Uses Fivetran SDK logging levels (`log.info`, `log.debug`, `log.warning`, `log.error`) for detailed sync visibility.

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

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code structure, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. Because this connector was generated from a third-party API mirror rather than MarginEdge's official docs, test thoroughly against a real MarginEdge account - in particular the `orders` date-filter/cursor field and the `attachments[]` field names noted above - before deploying to production. For inquiries, please reach out to our Support team.

## Resources

- [Fivetran Connector SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)
- [MarginEdge](https://www.marginedge.com/)
