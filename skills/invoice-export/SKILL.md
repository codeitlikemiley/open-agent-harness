---
name: invoice-export
description: Diagnose customer invoice CSV export failures.
---

When conversation `{{id}}` mentions invoice export:

1. Confirm the ticket id and billing period.
2. Check whether the export job is queued, failed, or never created.
3. Suggested reply: apologize, restate the missing CSV, and give a time window for a manual export.
