import os
import re

def restore_purple_theme(directory="templates"):
    """
    Restores the original purple color scheme to all HTML templates
    """
    
    # Restore original purple colors
    color_restorations = {
        # Restore primary purple colors
        "--primary-color: #64748b": "--primary-color: #667eea",
        "--primary-color: #3b82f6": "--primary-color: #667eea",
        "--primary-color: #0ea5e9": "--primary-color: #667eea",
        
        # Restore purple gradients
        "--primary-gradient: linear-gradient(135deg, #64748b 0%, #475569 100%)": 
            "--primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "--primary-gradient: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)": 
            "--primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "--primary-gradient: linear-gradient(135deg, #0ea5e9 0%, #06b6d4 100%)": 
            "--primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        
        # Restore pink secondary colors
        "--secondary-color: #94a3b8": "--secondary-color: #f093fb",
        "--secondary-color: #f59e0b": "--secondary-color: #f093fb",
        "--secondary-color: #10b981": "--secondary-color: #f093fb",
        
        # Restore pink gradients
        "--secondary-gradient: linear-gradient(135deg, #94a3b8 0%, #64748b 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)",
        "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)",
        "--secondary-gradient: linear-gradient(135deg, #10b981 0%, #059669 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)",
        
        # Restore other purple theme colors
        "--success-color: #10b981": "--success-color: #4facfe",
        "--warning-color: #f59e0b": "--warning-color: #ffd93d",
        "--danger-color: #ef4444": "--danger-color: #ff6b6b",
        
        # Restore original glass morphism
        "--glass-bg: rgba(15, 23, 42, 0.7)": "--glass-bg: rgba(255, 255, 255, 0.15)",
        "--glass-bg: rgba(17, 24, 39, 0.8)": "--glass-bg: rgba(255, 255, 255, 0.15)",
        "--glass-bg: rgba(15, 23, 42, 0.75)": "--glass-bg: rgba(255, 255, 255, 0.15)",
        
        # Restore glass borders
        "--glass-border: rgba(100, 116, 139, 0.2)": "--glass-border: rgba(255, 255, 255, 0.2)",
        "--glass-border: rgba(59, 130, 246, 0.3)": "--glass-border: rgba(255, 255, 255, 0.2)",
        "--glass-border: rgba(100, 116, 139, 0.25)": "--glass-border: rgba(255, 255, 255, 0.2)",
        
        # Restore original shadows
        "--shadow-light: 0 8px 32px rgba(15, 23, 42, 0.3)": 
            "--shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37)",
        "--shadow-light: 0 8px 32px rgba(59, 130, 246, 0.3)": 
            "--shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37)",
        "--shadow-light: 0 8px 32px rgba(14, 165, 233, 0.4)": 
            "--shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37)",
        
        # Restore original purple background gradients
        "background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%)":
            "background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)",
        "background: linear-gradient(135deg, #111827 0%, #1f2937 50%, #374151 100%)":
            "background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)",
        "background: linear-gradient(135deg, #0ea5e9 0%, #06b6d4 50%, #10b981 100%)":
            "background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)",
        "background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%)":
            "background: linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "background: linear-gradient(135deg, #111827 0%, #1f2937 100%)":
            "background: linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
    }
    
    # Check if templates directory exists
    if not os.path.exists(directory):
        print(f"❌ Directory '{directory}' not found!")
        return
    
    updated_files = []
    
    for filename in os.listdir(directory):
        if filename.endswith('.html'):
            filepath = os.path.join(directory, filename)
            
            try:
                # Read file content
                with open(filepath, 'r', encoding='utf-8') as file:
                    content = file.read()
                
                original_content = content
                
                # Restore purple colors
                for current_color, purple_color in color_restorations.items():
                    content = content.replace(current_color, purple_color)
                
                # Remove any enhanced dark/subtle theme styles that were added
                enhanced_styles_patterns = [
                    r'/\* Enhanced dark theme visibility \*/.*?(?=/\*|</style>)',
                    r'/\* Enhanced subtle theme styles \*/.*?(?=/\*|</style>)',
                ]
                
                for pattern in enhanced_styles_patterns:
                    content = re.sub(pattern, '', content, flags=re.DOTALL)
                
                # Only write if content actually changed
                if content != original_content:
                    with open(filepath, 'w', encoding='utf-8') as file:
                        file.write(content)
                    
                    updated_files.append(filename)
                    print(f"💜 Restored purple theme in {filename}")
                else:
                    print(f"⏭️  No changes needed for {filename}")
                    
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")
    
    # Summary
    if updated_files:
        print(f"\n💜 Successfully restored purple theme in {len(updated_files)} file(s):")
        for file in updated_files:
            print(f"   • {file}")
        print(f"\n✨ Original purple theme features restored:")
        print(f"   • Purple to pink gradients (#667eea → #f093fb)")
        print(f"   • Beautiful glass morphism effects")
        print(f"   • Vibrant purple backgrounds")
        print(f"   • Classic color combination")
    else:
        print("ℹ️  No files were updated. Templates may already use the purple theme.")

def create_purple_preview():
    """Create a preview to show the restored purple theme"""
    preview_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Purple Theme Restored - Preview</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        :root {
            /* Original Purple Color Scheme - RESTORED */
            --primary-color: #667eea;
            --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            --secondary-color: #f093fb;
            --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            --success-color: #4facfe;
            --warning-color: #ffd93d;
            --danger-color: #ff6b6b;
            --glass-bg: rgba(255, 255, 255, 0.15);
            --glass-border: rgba(255, 255, 255, 0.2);
            --shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37);
            --border-radius: 20px;
            --transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
            min-height: 100vh;
            color: white;
            padding: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .preview-container {
            text-align: center;
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            border-radius: var(--border-radius);
            padding: 60px;
            box-shadow: var(--shadow-light);
            max-width: 600px;
        }
        
        .preview-container h1 {
            font-size: 3rem;
            margin-bottom: 20px;
            background: linear-gradient(135deg, #ffffff 0%, #e2e8f0 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .color-demo {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 20px;
            margin: 30px 0;
        }
        
        .color-card {
            padding: 20px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.2);
        }
        
        .primary-demo { background: var(--primary-gradient); }
        .secondary-demo { background: var(--secondary-gradient); }
        
        .btn {
            display: inline-block;
            padding: 15px 30px;
            background: var(--primary-gradient);
            color: white;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            margin: 10px;
            cursor: pointer;
            transition: var(--transition);
        }
        
        .btn:hover {
            transform: translateY(-3px);
            box-shadow: var(--shadow-light);
        }
    </style>
</head>
<body>
    <div class="preview-container">
        <h1><i class="fas fa-palette"></i> Purple Theme Restored!</h1>
        <p style="font-size: 1.2rem; margin-bottom: 30px; opacity: 0.9;">
            Your beautiful original purple color scheme is back
        </p>
        
        <div class="color-demo">
            <div class="color-card primary-demo">
                <h3>Purple Primary</h3>
                <p>#667eea → #764ba2</p>
            </div>
            <div class="color-card secondary-demo">
                <h3>Pink Secondary</h3>
                <p>#f093fb → #f5576c</p>
            </div>
        </div>
        
        <button class="btn">Sample Button</button>
        
        <p style="margin-top: 30px; opacity: 0.8;">
            <i class="fas fa-check-circle" style="color: #4facfe;"></i>
            Original glass morphism and gradients restored
        </p>
    </div>
</body>
</html>"""
    
    with open('purple_theme_restored.html', 'w', encoding='utf-8') as file:
        file.write(preview_html)
    
    print("💜 Created 'purple_theme_restored.html' - Open to preview your restored theme!")

if __name__ == "__main__":
    print("💜 Purple Theme Restoration")
    print("=" * 35)
    
    # Restore purple theme
    restore_purple_theme()
    
    # Create preview
    create_purple_preview()
    
    print("\n🎉 Purple theme restoration complete!")
    print("🔍 Open 'purple_theme_restored.html' to preview")
    print("🚀 Restart your FastAPI server to see the beautiful purple colors!")
