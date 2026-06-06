import re

def scan():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.splitlines()
    output = []
    for i, line in enumerate(lines):
        if line.strip().startswith(("@app.get", "@app.post", "@app.put", "@app.delete", "@app.patch")):
            decorator_lines = []
            for j in range(i, len(lines)):
                decorator_lines.append(lines[j])
                if lines[j].strip().startswith("async def") or lines[j].strip().startswith("def"):
                    break
            decorator_str = "\n".join(decorator_lines)
            match = re.search(r'@app\.(get|post|put|delete|patch)\(\s*"([^"]+)"', decorator_str)
            if match:
                method = match.group(1).upper()
                path = match.group(2)
                has_hide = 'include_in_schema=False' in decorator_str
                output.append(f"{i+1:4}: {method:6} {path:60} | Hidden: {has_hide}")
                
    with open('scratch/all_routes.txt', 'w', encoding='utf-8') as f:
        f.write("\n".join(output) + "\n")

if __name__ == '__main__':
    scan()
