# 🏠 Statathon API Gateway

Welcome to the documentation for  **Statathon Dataset API Gateway**.
This project enables structured access to official survey microdata using:

* DDI-based ingestion of ZIP datasets
* Filtered SQL API queries (with role-based control)
* Usage metering + rate limits
* Cell suppression & aggregation (for privacy)
* Micro-payment integration(To be integrated)
* Swagger / Postman / MkDocs documentation

---

## Project Structure

| Folder/File   | Description                                     |
| ------------- | ----------------------------------------------- |
| `main.py`     | FastAPI app entrypoint                          |
| `routers/`    | Contains upload, query, and auth endpoints      |
| `auth/local/` | JWT-based auth system with role checks          |
| `utils/`      | Helpers for parsing DDI, converting, validating |
| `templates/`  | Contains the HTML files for all the pages.      |
| `docs/`       | MkDocs-based API documentation                  |
| `query/`      | Conains the route for query data                |

---

## How to Test

You can use **Postman** or Swagger UI (`/docs`) to test the APIs.

### 🔑 1. Login

```http
POST /auth/token
```

**Body (form):**

```
username: statathon12@gmail.com
password: yourpassword
```

Copy the `access_token` from response.

---

### 📤 2. Upload Dataset (Admin Only)

```http
POST /upload/
```

**Form Data:**

* file: NSS.zip
* schema: public

**Header:**

```
Authorization: Bearer <ADMIN_JWT>
```

---

### 📥 3. Query Dataset

```http
GET /datasets/{table_name}/query?limit=10&columns=A,B,C&filters=X=Y
```

**Headers:**

```
Authorization: Bearer <USER_OR_ADMIN_JWT>
Accept: application/json OR text/csv
```

---

### 📊 4. View Usage Logs (Admin SQL)

Use a database client to run:

```sql
SELECT user_email, queried_at::date, SUM(rows_returned) AS total_rows
FROM usage_logs
GROUP BY user_email, queried_at::date
ORDER BY queried_at DESC;
```

---

## 📚 Documentation Sections

* [API Reference](api.md)
* [Authentication](auth.md)
* [Usage Metering](usage.md)

Enjoy building privacy-compliant data systems 🚀
