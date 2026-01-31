# 🔐 Authentication API

This document describes how to register, log in, and use authentication headers for **Statathon API Gateway**.

---

## 🔑 Register a New User

**POST** `/auth/register`

Create a new user account.

### 📥 Body (JSON):

```json
{
  "username": "Sudheer",
  "email": "statathon12@gmail.com",
  "password": "mypassword",
  "role_id": 3
}
```

### 🎭 Roles:

* `1` → Admin
* `3` → General User

### Response:

```json
{
  "message": "User created successfully"
}
```

---

## 🔐 Get Access Token

**POST** `/auth/token`

Use email + password to get JWT token.

### 📥 Form Data:

| Key      | Type   | Example                                               |
| -------- | ------ | ----------------------------------------------------- |
| username | string | [statathon12@gmail.com](mailto:statathon12@gmail.com) |
| password | string | mypassword                                            |

### Response:

```json
{
  "access_token": "<JWT_TOKEN>",
  "token_type": "bearer"
}
```

---

## 📎 Using Bearer Token

After login, use the returned `access_token` in all protected routes.

### 🔐 Header Example:

```http
Authorization: Bearer <JWT_TOKEN>
```

---

## 🔁 Token Lifetime

* JWT tokens are valid for **30 minutes** by default.
* They are **stateless** and do not require server session storage.

---

## 🚫 Unauthorized Access

If the token is invalid or expired:

**Response:**

```json
{
  "detail": "Could not validate credentials"
}
```

---

## 🔍 Who Am I? (Debug)

**GET** `/debug/user-info`

Check which user is currently authenticated.

### Example:

```json
{
  "username": "statathon12@gmail.com",
  "role": 1,
  "is_admin": true
}
```

---

Return to [API Reference](api.md) →
