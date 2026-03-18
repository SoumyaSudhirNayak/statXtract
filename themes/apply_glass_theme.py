import os
import shutil
import re

# Design Tokens from metadata_detail.html + refinements for full UI
GLASS_THEME_CSS = """
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <style id="statxtract-glass-theme">
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --secondary: #64748b;
            --accent: #10b981;
            --bg-dark: #0f172a;
            --glass-bg: rgba(30, 41, 59, 0.7);
            --glass-border: rgba(255, 255, 255, 0.1);
            --text-main: rgba(255, 255, 255, 0.95);
            --text-muted: rgba(255, 255, 255, 0.7);
            --shadow-heavy: 0 20px 25px -5px rgba(0, 0, 0, 0.3), 0 10px 10px -5px rgba(0, 0, 0, 0.2);
            --border-radius: 24px;
            --transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Plus Jakarta Sans', sans-serif;
        }

        body {
            background: var(--bg-dark) !important;
            background-image: 
                radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.15) 0px, transparent 50%),
                radial-gradient(at 100% 0%, rgba(16, 185, 129, 0.1) 0px, transparent 50%) !important;
            min-height: 100vh;
            color: var(--text-main) !important;
            transition: var(--transition);
            overflow-x: hidden;
        }

        .main-container {
            max-width: 1200px !important;
            margin: 0 auto !important;
            padding: 40px 20px !important;
        }

        /* Glass Morphism Cards */
        .glass-card, .card, .stat-card, .welcome-message, .user-info {
            background: var(--glass-bg) !important;
            backdrop-filter: blur(12px) !important;
            border: 1px solid var(--glass-border) !important;
            border-radius: var(--border-radius) !important;
            box-shadow: var(--shadow-heavy) !important;
            transition: var(--transition) !important;
            color: var(--text-main) !important;
            padding: 24px !important;
        }

        .glass-card:hover {
            transform: translateY(-8px) !important;
            border-color: var(--primary) !important;
            background: rgba(255, 255, 255, 0.05) !important;
        }

        .card-icon {
            font-size: 2rem !important;
            margin-bottom: 20px !important;
            color: var(--primary) !important;
        }

        .card-arrow {
            position: absolute !important;
            bottom: 24px !important;
            right: 24px !important;
            color: var(--text-muted) !important;
            transition: var(--transition) !important;
        }

        .glass-card:hover .card-arrow {
            color: var(--primary) !important;
            transform: translateX(5px) !important;
        }

        /* Scrollable Content */
        .scroll-content {
            overflow-y: auto !important;
            max-height: 80vh !important;
            padding-right: 10px !important;
        }

        /* Headers */
        .header, .welcome-stats {
            background: var(--glass-bg) !important;
            backdrop-filter: blur(20px) !important;
            border: 1px solid var(--glass-border) !important;
            border-radius: var(--border-radius) !important;
            padding: 40px !important;
            margin-bottom: 40px !important;
            display: flex !important;
            flex-direction: column !important;
            gap: 10px !important;
        }

        h1, h2, h3, .card-title, .stat-number {
            background: linear-gradient(135deg, #fff 0%, #cbd5e1 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800 !important;
        }

        .card-description, .stat-label, p {
            color: var(--text-muted) !important;
        }

        /* Buttons */
        .btn, button, .logout-btn, .theme-toggle {
            background: var(--glass-bg) !important;
            border: 1px solid var(--glass-border) !important;
            border-radius: 12px !important;
            color: var(--text-main) !important;
            font-weight: 600 !important;
            padding: 12px 24px !important;
            cursor: pointer !important;
            transition: var(--transition) !important;
            text-decoration: none !important;
            display: inline-flex !important;
            align-items: center !important;
            gap: 8px !important;
        }

        .btn-primary, .btn-submit, button[type="submit"] {
            background: var(--primary) !important;
            border: none !important;
            color: white !important;
        }

        .btn:hover, button:hover, .logout-btn:hover, .theme-toggle:hover {
            background: rgba(255, 255, 255, 0.1) !important;
            transform: translateY(-2px) !important;
        }

        .btn-primary:hover, .btn-submit:hover, button[type="submit"]:hover {
            background: var(--primary-dark) !important;
            box-shadow: 0 4px 12px rgba(99, 102, 241, 0.4) !important;
        }

        /* Badges */
        .admin-badge, .badge {
            background: rgba(99, 102, 241, 0.1) !important;
            border: 1px solid var(--primary) !important;
            color: var(--primary) !important;
            padding: 6px 12px !important;
            border-radius: 20px !important;
            font-size: 0.8rem !important;
            font-weight: 700 !important;
            display: inline-flex !important;
            align-items: center !important;
            gap: 5px !important;
        }

        /* Forms */
        .form-group {
            margin-bottom: 20px !important;
        }

        .form-label {
            display: block !important;
            margin-bottom: 8px !important;
            color: var(--text-muted) !important;
            font-weight: 500 !important;
        }

        .form-control, .form-select, .form-input, input, select, textarea {
            background: rgba(255, 255, 255, 0.05) !important;
            border: 1px solid var(--glass-border) !important;
            border-radius: 12px !important;
            color: var(--text-main) !important;
            padding: 12px 20px !important;
            width: 100% !important;
        }

        .form-control:focus, .form-select:focus, input:focus {
            border-color: var(--primary) !important;
            box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.2) !important;
            outline: none !important;
        }

        /* Stats Grid */
        .stats-container, .dashboard-grid {
            display: grid !important;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)) !important;
            gap: 24px !important;
            margin-bottom: 40px !important;
        }

        /* Tables */
        .table-container {
            background: var(--glass-bg) !important;
            border-radius: var(--border-radius) !important;
            padding: 24px !important;
            overflow-x: auto !important;
        }

        table {
            width: 100% !important;
            border-collapse: collapse !important;
            background: transparent !important;
        }

        th {
            text-align: left !important;
            padding: 16px !important;
            color: var(--primary) !important;
            font-weight: 700 !important;
            border-bottom: 1px solid var(--glass-border) !important;
        }

        td {
            padding: 16px !important;
            color: var(--text-main) !important;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05) !important;
        }

        tr:hover td {
            background: rgba(255, 255, 255, 0.02) !important;
        }

        /* Toasts */
        .toast {
            background: var(--glass-bg) !important;
            backdrop-filter: blur(20px) !important;
            border: 1px solid var(--glass-border) !important;
            border-radius: 16px !important;
            box-shadow: var(--shadow-heavy) !important;
            color: var(--text-main) !important;
        }

        /* Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
        }

        ::-webkit-scrollbar-track {
            background: transparent;
        }

        ::-webkit-scrollbar-thumb {
            background: var(--glass-border);
            border-radius: 10px;
        }

        ::-webkit-scrollbar-thumb:hover {
            background: var(--primary);
        }

        /* Controls Positions */
        .logout-btn {
            position: fixed !important;
            top: 20px !important;
            right: 20px !important;
            z-index: 1000 !important;
        }

        .theme-toggle {
            position: fixed !important;
            top: 20px !important;
            right: 160px !important;
            z-index: 1000 !important;
        }

        @media (max-width: 768px) {
            .theme-toggle { right: 140px !important; }
            .logout-btn span, .theme-toggle span { display: none !important; }
            .main-container { padding: 20px 10px !important; }
        }
    </style>
"""

TEMPLATES_DIR = "templates"
BACKUP_DIR = "templates/backups_original"

FILES_TO_THEME = [
    "admin_dashboard.html",
    "index.html",
    "login.html",
    "register.html",
    "query_ui.html",
    "usage.html",
    "datasets.html",
    "schemas.html",
    "upload_progress.html"
]

def apply_theme():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        print(f"Created backup directory: {BACKUP_DIR}")

    for filename in FILES_TO_THEME:
        file_path = os.path.join(TEMPLATES_DIR, filename)
        if not os.path.exists(file_path):
            print(f"Skipping {filename} (not found)")
            continue

        # Backup if not already backed up
        backup_path = os.path.join(BACKUP_DIR, filename)
        if not os.path.exists(backup_path):
            shutil.copy2(file_path, backup_path)
            print(f"Backed up {filename} to {backup_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 1. Remove existing glass theme if already applied to avoid duplicates
        content = re.sub(r'<style id="statxtract-glass-theme">.*?</style>', '', content, flags=re.DOTALL)
        
        # 2. Remove other style blocks (to start fresh)
        content = re.sub(r'<style>.*?</style>', '', content, flags=re.DOTALL)
        
        # 3. Remove Inter font links
        content = re.sub(r'<link href="https://fonts\.googleapis\.com/css2\?family=Inter:.*?rel="stylesheet">', '', content)
        
        # 4. Inject new theme into <head>
        if '</head>' in content:
            content = content.replace('head>', f'head>\n{GLASS_THEME_CSS}')
        else:
            print(f"Warning: Could not find </head> in {filename}")

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Applied StatXtract Glass Theme to {filename}")

if __name__ == "__main__":
    apply_theme()
    print("\\nTheme applied successfully! Refresh your browser to see the changes.")
