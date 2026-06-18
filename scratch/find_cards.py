with open(r"e:\STATATHON 2025 LOCAL\Statathon_API_Gateway\templates\batch_import_ui.html", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "/api/admin/batch-import" in line or "status" in line.lower() or "poll" in line.lower():
        if len(line.strip()) < 150:
            print(f"Line {i+1}: {line.strip()}")
