import pytest
from fastapi.testclient import TestClient
from main import app
from unittest.mock import AsyncMock, patch, MagicMock

client = TestClient(app)

@pytest.fixture
def mock_db():
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_acquire = MagicMock()
    mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire.__aexit__ = AsyncMock(return_value=None)
    mock_pool.acquire = MagicMock(return_value=mock_acquire)
    
    app.state.db = mock_pool
    return mock_conn

def solve_captcha(client_instance):
    res = client_instance.get("/auth/captcha")
    data = res.json()
    expr = data["question"].replace("= ?", "").strip()
    parts = expr.split()
    num1 = int(parts[0])
    op = parts[1]
    num2 = int(parts[2])
    if op == "+":
        return str(num1 + num2)
    else:
        return str(num1 - num2)

@patch("auth.local.routes.get_user_by_email")
@patch("auth.local.routes.verify_password")
def test_admin_portal_login_matrix(mock_verify_password, mock_get_user, mock_db):
    mock_verify_password.return_value = True

    # Case 1: User account logs in to Admin Portal -> Should reject
    mock_get_user.return_value = {
        "email": "user@example.com",
        "hashed_password": "hashed_password",
        "role_id": "3",  # General User
        "is_blocked": False,
        "is_verified": True
    }
    response = client.post(
        "/auth/login",
        data={"username": "user@example.com", "password": "password"},
        headers={"referer": "http://testserver/login"},
        follow_redirects=False
    )
    assert response.status_code == 302
    assert "error=Access+Denied.+This+portal+is+reserved+for+Administrator+accounts." in response.headers["location"]

    # Case 2: Admin account logs in to Admin Portal -> Should succeed and redirect to Admin Dashboard
    mock_get_user.return_value = {
        "email": "admin@example.com",
        "hashed_password": "hashed_password",
        "role_id": "1",  # Admin
        "is_blocked": False,
        "is_verified": True
    }
    response = client.post(
        "/auth/login",
        data={"username": "admin@example.com", "password": "password"},
        headers={"referer": "http://testserver/login"},
        follow_redirects=False
    )
    assert response.status_code == 302
    assert "/admin/dashboard" in response.headers["location"]

@patch("auth.local.routes.get_user_by_email")
@patch("auth.local.routes.verify_password")
def test_user_portal_login_matrix(mock_verify_password, mock_get_user, mock_db):
    mock_verify_password.return_value = True

    # Case 3: Admin account logs in to User Portal -> Should reject
    mock_get_user.return_value = {
        "email": "admin@example.com",
        "hashed_password": "hashed_password",
        "role_id": "1",  # Admin
        "is_blocked": False,
        "is_verified": True
    }
    
    # We use a session with TestClient
    with TestClient(app) as test_client:
        # Solve CAPTCHA to get a valid session cookie with captcha registered
        ans = solve_captcha(test_client)
        
        response = test_client.post(
            "/auth/login",
            data={
                "username": "admin@example.com",
                "password": "password",
                "captcha_answer": ans
            },
            headers={"referer": "http://testserver/user/login"},
            follow_redirects=False
        )
        print("Admin login response text:", response.text)
        assert response.status_code == 302
        assert "error=Access+Denied.+Please+log+in+through+the+User+Portal." in response.headers["location"]

    # Case 4: User account logs in to User Portal -> Should succeed and redirect to User Dashboard
    mock_get_user.return_value = {
        "email": "user@example.com",
        "hashed_password": "hashed_password",
        "role_id": "3",  # General User
        "is_blocked": False,
        "is_verified": True
    }
    with TestClient(app) as test_client:
        ans = solve_captcha(test_client)
        
        response = test_client.post(
            "/auth/login",
            data={
                "username": "user@example.com",
                "password": "password",
                "captcha_answer": ans
            },
            headers={"referer": "http://testserver/user/login"},
            follow_redirects=False
        )
        print("User login response text:", response.text)
        assert response.status_code == 302
        assert "/user/dashboard" in response.headers["location"]
