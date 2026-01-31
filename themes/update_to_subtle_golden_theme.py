import os
import re

def update_template_colors(directory="templates"):
    """
    Updates all HTML templates to use a Subtle Professional Theme with Golden Touch
    """
    
    # Subtle professional color mappings with golden accents
    old_colors = {
        # Replace bright blues with subtle slate colors
        "--primary-color: #3b82f6": "--primary-color: #64748b",
        "--primary-color: #667eea": "--primary-color: #64748b",
        "--primary-color: #0ea5e9": "--primary-color: #64748b",
        
        # Replace primary gradients
        "--primary-gradient: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)": 
            "--primary-gradient: linear-gradient(135deg, #64748b 0%, #475569 100%)",
        "--primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%)": 
            "--primary-gradient: linear-gradient(135deg, #64748b 0%, #475569 100%)",
        "--primary-gradient: linear-gradient(135deg, #0ea5e9 0%, #06b6d4 100%)": 
            "--primary-gradient: linear-gradient(135deg, #64748b 0%, #475569 100%)",
        
        # Replace secondary colors with golden touch
        "--secondary-color: #f59e0b": "--secondary-color: #f59e0b",  # Keep golden
        "--secondary-color: #f093fb": "--secondary-color: #f59e0b",  # Purple to golden
        "--secondary-color: #10b981": "--secondary-color: #f59e0b",  # Green to golden
        "--secondary-color: #94a3b8": "--secondary-color: #f59e0b",  # Gray to golden
        
        # Replace secondary gradients with golden gradients
        "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",  # Keep golden
        "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",  # Pink to golden
        "--secondary-gradient: linear-gradient(135deg, #10b981 0%, #059669 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",  # Green to golden
        "--secondary-gradient: linear-gradient(135deg, #94a3b8 0%, #64748b 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",  # Gray to golden
        
        # Update success color to golden accent instead of green
        "--success-color: #10b981": "--success-color: #f59e0b",  # Green to golden
        "--success-color: #22c55e": "--success-color: #f59e0b",  # Green to golden
        "--success-color: #4facfe": "--success-color: #f59e0b",  # Blue to golden
        
        # Keep warning as golden (since it's already golden)
        "--warning-color: #ffd93d": "--warning-color: #f59e0b",  # Bright yellow to sophisticated golden
        
        # Keep danger as red
        "--danger-color: #ff6b6b": "--danger-color: #ef4444",
        "--danger-color: #ef4444": "--danger-color: #ef4444",  # Keep red
        
        # Update glass morphism for subtle theme
        "--glass-bg: rgba(17, 24, 39, 0.8)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        "--glass-bg: rgba(255, 255, 255, 0.15)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        "--glass-bg: rgba(255, 255, 255, 0.2)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        
        # Update glass borders with subtle golden tint
        "--glass-border: rgba(59, 130, 246, 0.3)": "--glass-border: rgba(100, 116, 139, 0.25)",
        "--glass-border: rgba(255, 255, 255, 0.2)": "--glass-border: rgba(100, 116, 139, 0.25)",
        "--glass-border: rgba(255, 255, 255, 0.3)": "--glass-border: rgba(100, 116, 139, 0.25)",
        
        # Update shadows for subtle theme
        "--shadow-light: 0 8px 32px rgba(59, 130, 246, 0.3)": 
            "--shadow-light: 0 8px 32px rgba(15, 23, 42, 0.3)",
        "--shadow-light: 0 8px 32px rgba(14, 165, 233, 0.4)": 
            "--shadow-light: 0 8px 32px rgba(15, 23, 42, 0.3)",
        "--shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37)": 
            "--shadow-light: 0 8px 32px rgba(15, 23, 42, 0.3)",
        
        # Update background gradients to subtle charcoal
        "background: linear-gradient(135deg, #111827 0%, #1f2937 50%, #374151 100%)":
            "background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%)",
        "background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)":
            "background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%)",
        "background: linear-gradient(135deg, #0ea5e9 0%, #06b6d4 50%, #10b981 100%)":
            "background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%)",
        "background: linear-gradient(135deg, #111827 0%, #1f2937 100%)":
            "background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%)",
    }
    
    # Enhanced subtle styles with golden accents
    enhanced_golden_styles = """
        /* Enhanced subtle theme with golden touch */
        .glass-card {
            background: rgba(15, 23, 42, 0.75);
            backdrop-filter: blur(20px) saturate(120%);
            border: 1px solid rgba(100, 116, 139, 0.25);
        }

        .glass-card:hover {
            background: rgba(15, 23, 42, 0.85);
            border-color: rgba(245, 158, 11, 0.3);
            transform: translateY(-3px);
            box-shadow: 0 12px 30px rgba(245, 158, 11, 0.1);
        }

        .btn {
            background: var(--primary-gradient);
            border: 1px solid rgba(100, 116, 139, 0.3);
            box-shadow: 0 4px 12px rgba(15, 23, 42, 0.2);
        }

        .btn:hover {
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.3);
            transform: translateY(-2px);
        }

        /* Golden accent buttons */
        .btn-secondary,
        .btn.secondary {
            background: var(--secondary-gradient);
            box-shadow: 0 4px 12px rgba(245, 158, 11, 0.2);
        }

        .btn-secondary:hover,
        .btn.secondary:hover {
            box-shadow: 0 8px 20px rgba(245, 158, 11, 0.3);
        }

        .form-input {
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid rgba(100, 116, 139, 0.3);
        }

        .form-input:focus {
            background: rgba(15, 23, 42, 0.8);
            border-color: #f59e0b;
            box-shadow: 0 0 0 3px rgba(245, 158, 11, 0.2);
        }

        /* Golden success states */
        .success-message,
        .toast.success {
            background: rgba(245, 158, 11, 0.2);
            border-color: var(--secondary-color);
            color: #fbbf24;
        }

        /* Golden badges and highlights */
        .stat-badge {
            background: rgba(245, 158, 11, 0.2);
            border: 1px solid var(--secondary-color);
            color: #fbbf24;
        }

        /* Golden icon accents */
        .card-icon,
        .schema-icon,
        .table-icon {
            color: var(--secondary-color);
        }

        /* Subtle golden highlights */
        .user-role {
            background: rgba(245, 158, 11, 0.2);
            border: 1px solid rgba(245, 158, 11, 0.3);
            color: #fbbf24;
        }"""
    
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
                
                # Replace colors
                for old, new in old_colors.items():
                    content = content.replace(old, new)
                
                # Add enhanced golden styles if not already present
                if '</style>' in content and enhanced_golden_styles not in content:
                    content = content.replace('</style>', f'{enhanced_golden_styles}\n    </style>')
                
                # Only write if content actually changed
                if content != original_content:
                    with open(filepath, 'w', encoding='utf-8') as file:
                        file.write(content)
                    
                    updated_files.append(filename)
                    print(f"✨ Updated {filename} with subtle golden theme")
                else:
                    print(f"⏭️  No changes needed for {filename}")
                    
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")
    
    # Summary
    if updated_files:
        print(f"\n🌟 Successfully updated {len(updated_files)} file(s) to Subtle Golden Theme:")
        for file in updated_files:
            print(f"   • {file}")
        print(f"\n✨ Golden touch features applied:")
        print(f"   • Soft charcoal backgrounds (#0f172a → #334155)")
        print(f"   • Muted slate blue primary (#64748b)")
        print(f"   • Elegant golden accents (#f59e0b)")
        print(f"   • Golden hover effects and highlights")
        print(f"   • Professional, sophisticated appearance")
    else:
        print("ℹ️  No files were updated. Templates may already use the golden theme.")

def create_golden_preview():
    """Create a preview to show the golden theme"""
    preview_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Subtle Golden Theme Preview</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        :root {
            --primary-color: #64748b;
            --primary-gradient: linear-gradient(135deg, #64748b 0%, #475569 100%);
            --secondary-color: #f59e0b;
            --secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
            --glass-bg: rgba(15, 23, 42, 0.7);
            --glass-border: rgba(100, 116, 139, 0.25);
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
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
            border-radius: 20px;
            padding: 60px;
            max-width: 600px;
        }
        
        .preview-container h1 {
            font-size: 3rem;
            margin-bottom: 20px;
            color: white;
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
            border: 1px solid rgba(255,255,255,0.1);
        }
        
        .primary-demo { 
            background: var(--primary-gradient);
            border-left: 4px solid var(--primary-color);
        }
        .golden-demo { 
            background: var(--secondary-gradient);
            border-left: 4px solid var(--secondary-color);
        }
        
        .btn {
            display: inline-block;
            padding: 15px 30px;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            margin: 10px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        
        .btn-primary {
            background: var(--primary-gradient);
            color: white;
        }
        
        .btn-golden {
            background: var(--secondary-gradient);
            color: white;
        }
        
        .btn:hover {
            transform: translateY(-2px);
        }
        
        .golden-icon {
            color: #f59e0b;
            font-size: 1.5rem;
            margin: 0 10px;
        }
    </style>
</head>
<body>
    <div class="preview-container">
        <h1><i class="fas fa-star golden-icon"></i> Subtle Golden Theme</h1>
        <p style="font-size: 1.2rem; margin-bottom: 30px; opacity: 0.9;">
            Professional charcoal with elegant golden accents
        </p>
        
        <div class="color-demo">
            <div class="color-card primary-demo">
                <h3>Slate Primary</h3>
                <p>#64748b</p>
                <i class="fas fa-palette" style="color: #64748b; font-size: 2rem; margin-top: 10px;"></i>
            </div>
            <div class="color-card golden-demo">
                <h3>Golden Touch</h3>
                <p>#f59e0b</p>
                <i class="fas fa-gem" style="color: #f59e0b; font-size: 2rem; margin-top: 10px;"></i>
            </div>
        </div>
        
        <button class="btn btn-primary">Primary Button</button>
        <button class="btn btn-golden">Golden Button</button>
        
        <p style="margin-top: 30px; opacity: 0.8;">
            <i class="fas fa-check-circle golden-icon"></i>
            Sophisticated and professional with golden elegance
        </p>
    </div>
</body>
</html>"""
    
    with open('golden_theme_preview.html', 'w', encoding='utf-8') as file:
        file.write(preview_html)
    
    print("🌟 Created 'golden_theme_preview.html' - Open to preview your golden theme!")

if __name__ == "__main__":
    print("🌟 Subtle Professional Theme with Golden Touch")
    print("=" * 50)
    
    # Update templates
    update_template_colors()
    
    # Create preview
    create_golden_preview()
    
    print("\n✨ Golden theme update complete!")
    print("🔍 Open 'golden_theme_preview.html' to preview")
    print("🚀 Restart your FastAPI server to see the golden elegance!")
