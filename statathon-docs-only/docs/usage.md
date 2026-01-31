# 📊 Usage Metering

This section describes how the API Gateway tracks and limits usage for each user based on the number of rows accessed per day.

---

## ✅ Row-Based Usage Limit

Each user has a maximum number of rows they can access per day. This is tracked via a `usage_logs` table.

### 🔹 Default Limits:

| Role          | Max Rows per Day |
| ------------- | ---------------- |
| Admin         | Unlimited        |
| User (role=3) | 100,000 rows     |

Once the limit is exceeded, further requests are blocked for that day.

---

## 📦 Usage Logging

Every data query (GET or POST) is logged with:

* `user_email`
* `endpoint`
* `schema_name`
* `table_name`
* `rows_returned`
* `bytes_sent`
* `timestamp`

These logs are stored in the `usage_logs` table in PostgreSQL.

---

## 🧪 Example: User Limit Exceeded

**Request:**

```
GET /datasets/table_name/query?limit=10000
```

**Response (after hitting daily quota):**

```json
{
  "detail": "Daily usage limit exceeded"
}
```

---

## 🛡️ Suppressed Query Still Logged

Even when a query is suppressed (for <5 rows), a log entry is still made with:

* `rows_returned = 0`
* `bytes_sent = 0`

---

## 📌 Admin Debug Tools

### 🔎 Endpoint: `/debug/user-info`

Returns your role and email for debugging.

### 📊 DB Table: `usage_logs`

You can query this table to generate usage analytics, billing, or throttling reports.

---

Continue to [Project Layout](index.md) →
