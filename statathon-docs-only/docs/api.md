# API Reference

This section documents the core APIs of  **Statathon Dataset Gateway**.

---

## Query a Dataset

**GET** `/datasets/{table_name}/query`

Use this endpoint to query any dataset table with filters, column selection, and pagination.

### 🔹 Parameters:

| Param     | Type    | Description                                                 |
| --------- | ------- | ----------------------------------------------------------- |
| `columns` | string  | Comma-separated list of columns to return                   |
| `filters` | string  | Filter expression using SQL operators (`=`, `>`, `<`, `IN`) |
| `limit`   | integer | Number of rows to return (default: 100)                     |
| `offset`  | integer | Number of rows to skip (default: 0)                         |

### 🔐 Auth Required:

* Bearer token (admin or user)

---

### Example 1: Admin (Full JSON Response)

**Request:**

```
GET /datasets/block_5_2_usual_subsidiary_economic_activity_particulars_of_hou/query?limit=10&offset=0&columns=Round_Centre_Code,FSU_Serial_No,Round,Sch_No
```

**Headers:**

```
Authorization: Bearer <ADMIN_JWT>
Accept: application/json
```

**Response:**

```json
[
  {
    "Round_Centre_Code": 12,
    "FSU_Serial_No": 71558,
    "Round": 68,
    "Sch_No": 100
  },
  {
    "Round_Centre_Code": 6,
    "FSU_Serial_No": 71560,
    "Round": 68,
    "Sch_No": 101
  }
]
```

---

### Example 2: CSV Download

**Request:**

```
GET /datasets/block_5_2_usual_subsidiary_economic_activity_particulars_of_hou/query?limit=10&columns=Round_Centre_Code,FSU_Serial_No,Round,Sch_No
```

**Headers:**

```
Authorization: Bearer <JWT>
Accept: text/csv
```

**Response (CSV):**

```
Round_Centre_Code,FSU_Serial_No,Round,Sch_No
12,71558,68,100
6,71560,68,101
```

---

### Example 3: Non-Admin (Suppression Triggered)

**Request:**

```
GET /datasets/block_5_2_usual_subsidiary_economic_activity_particulars_of_hou/query?filters=FSU_Serial_No=71238;Round_Centre_Code=4&limit=10
```

**Headers:**

```
Authorization: Bearer <USER_JWT>
Accept: application/json
```

**Response (403 Forbidden):**

```json
{
  "detail": {
    "error": "Data Access Restricted",
    "detail": "Data suppressed: Result contains 1 rows, which is below the minimum threshold of 5 rows required for privacy protection.",
    "code": "CELL_SUPPRESSION_APPLIED",
    "minimum_rows_required": 5,
    "actual_rows": 1
  }
}
```

---

## 📄 Upload Dataset

**POST** `/upload/`

Upload a ZIP file containing `.csv`, `.txt`, and `.xml` (DDI) files.

### 🔹 Form Data:

| Key      | Type   | Description                            |
| -------- | ------ | -------------------------------------- |
| `file`   | file   | ZIP file with dataset + DDI            |
| `schema` | string | Target schema (e.g., `public`, `plfs`) |

### 🔐 Auth Required:

* Admin only

**Response:**

```html
<h3>✅ Upload successful!</h3>
```

---

## 🔍 View Current User (Debug)

**GET** `/debug/user-info`

Returns current user’s email, role, and debug info.

**Response:**

```json
{
  "username": "statathon12@gmail.com",
  "role": 1,
  "is_admin": true
}
```

---

## 🔎 Test Suppression Logic (Debug)

**GET** `/debug/test-suppression`

Simulates a query returning <5 rows and checks suppression logic.

**Response (for users):**

```json
{
  "message": "✅ Cell suppression is working correctly!",
  "suppressed": true,
  "user_role": "3",
  "row_count": 3
}
```

---

Continue to [Authentication](auth.md) →
