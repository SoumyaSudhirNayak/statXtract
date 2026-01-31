import os
import re

def update_template_colors(directory="templates"):
    """
    Updates all HTML templates to use the Elegant Dark Theme
    """
    
    # Dark theme color mappings
    old_colors = {
        # Primary colors (purple to blue)
        "--primary-color: #667eea": "--primary-color: #3b82f6",
        "--primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%)": 
            "--primary-gradient: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)",
        
        # Secondary colors (pink to golden)
        "--secondary-color: #f093fb": "--secondary-color: #f59e0b",
        "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)":
            "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",
        
        # Other accent colors
        "--success-color: #4facfe": "--success-color: #10b981",
        "--warning-color: #ffd93d": "--warning-color: #f59e0b",
        "--danger-color: #ff6b6b": "--danger-color: #ef4444",
        
        # Glass morphism updates for dark theme
        "--glass-bg: rgba(255, 255, 255, 0.15)": "--glass-bg: rgba(17, 24, 39, 0.8)",
        "--glass-border: rgba(255, 255, 255, 0.2)": "--glass-border: rgba(59, 130, 246, 0.3)",
        
        # Shadow updates
        "--shadow-light: 0 8px 32px rgba(102, 126, 234, 0.37)": 
            "--shadow-light: 0 8px 32px rgba(59, 130, 246, 0.3)",
        "--shadow-heavy: 0 20px 40px rgba(0, 0, 0, 0.1)": 
            "--shadow-heavy: 0 20px 40px rgba(0, 0, 0, 0.4)",
        
        # Background gradients (most important for dark theme)
        "background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%)":
            "background: linear-gradient(135deg, #111827 0%, #1f2937 50%, #374151 100%)",
        "background: linear-gradient(135deg, #667eea 0%, #764ba2 100%)":
            "background: linear-gradient(135deg, #111827 0%, #1f2937 100%)",
        
        # Also handle blue-teal colors if they exist (from previous updates)
        "background: linear-gradient(135deg, #0ea5e9 0%, #06b6d4 50%, #10b981 100%)":
            "background: linear-gradient(135deg, #111827 0%, #1f2937 50%, #374151 100%)",
        "--primary-color: #0ea5e9": "--primary-color: #3b82f6",
        "--secondary-color: #10b981": "--secondary-color: #f59e0b",
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
                
                # Replace colors
                for old, new in old_colors.items():
                    content = content.replace(old, new)
                
                # Only write if content actually changed
                if content != original_content:
                    with open(filepath, 'w', encoding='utf-8') as file:
                        file.write(content)
                    
                    updated_files.append(filename)
                    print(f"✅ Updated {filename} with dark theme colors")
                else:
                    print(f"⏭️  No changes needed for {filename}")
                    
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")
    
    # Summary
    if updated_files:
        print(f"\n🌙 Successfully updated {len(updated_files)} file(s) to Elegant Dark Theme:")
        for file in updated_files:
            print(f"   • {file}")
        print(f"\n✨ Dark theme features applied:")
        print(f"   • Navy/gray backgrounds (#111827 → #374151)")
        print(f"   • Blue primary accents (#3b82f6)")
        print(f"   • Golden secondary highlights (#f59e0b)")
        print(f"   • Enhanced dark glass morphism")
    else:
        print("ℹ️  No files were updated. Templates may already use the dark theme.")

def verify_dark_theme_application(directory="templates"):
    """
    Verify that dark theme colors are properly applied
    """
    print("\n🔍 Verifying dark theme application...")
    
    dark_theme_indicators = [
        "#111827",  # Dark background
        "#3b82f6",  # Blue primary
        "#f59e0b",  # Golden secondary
        "rgba(17, 24, 39, 0.8)"  # Dark glass
    ]
    
    for filename in os.listdir(directory):
        if filename.endswith('.html'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
            
            found_indicators = [indicator for indicator in dark_theme_indicators if indicator in content]
            
            if found_indicators:
                print(f"✅ {filename}: Dark theme detected ({len(found_indicators)}/4 indicators)")
            else:
                print(f"⚠️  {filename}: No dark theme indicators found")

if __name__ == "__main__":
    print("🌙 Elegant Dark Theme Updater")
    print("=" * 40)
    
    # Update templates
    update_template_colors()
    
    # Verify the changes
    verify_dark_theme_application()
    
    print("\n🚀 Run your FastAPI server to see the dark theme!")
    print("💡 If templates don't change, try:")
    print("   1. Hard refresh (Ctrl+F5)")
    print("   2. Restart FastAPI with --reload")
    print("   3. Clear browser cache")
