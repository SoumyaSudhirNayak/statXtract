import os
import re

def update_template_colors(directory="templates"):
    """
    Updates all HTML templates to use a Subtle Professional Theme
    """
    
    # Subtle professional color mappings
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
        
        # Replace bright secondary colors with muted ones
        "--secondary-color: #f59e0b": "--secondary-color: #94a3b8",
        "--secondary-color: #f093fb": "--secondary-color: #94a3b8",
        "--secondary-color: #10b981": "--secondary-color: #94a3b8",
        
        # Replace secondary gradients
        "--secondary-gradient: linear-gradient(135deg, #f59e0b 0%, #d97706 100%)":
            "--secondary-gradient: linear-gradient(135deg, #94a3b8 0%, #64748b 100%)",
        "--secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%)":
            "--secondary-gradient: linear-gradient(135deg, #94a3b8 0%, #64748b 100%)",
        "--secondary-gradient: linear-gradient(135deg, #10b981 0%, #059669 100%)":
            "--secondary-gradient: linear-gradient(135deg, #94a3b8 0%, #64748b 100%)",
        
        # Update glass morphism for subtle theme
        "--glass-bg: rgba(17, 24, 39, 0.8)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        "--glass-bg: rgba(255, 255, 255, 0.15)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        "--glass-bg: rgba(255, 255, 255, 0.2)": "--glass-bg: rgba(15, 23, 42, 0.7)",
        
        # Update glass borders
        "--glass-border: rgba(59, 130, 246, 0.3)": "--glass-border: rgba(100, 116, 139, 0.2)",
        "--glass-border: rgba(255, 255, 255, 0.2)": "--glass-border: rgba(100, 116, 139, 0.2)",
        "--glass-border: rgba(255, 255, 255, 0.3)": "--glass-border: rgba(100, 116, 139, 0.2)",
        
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
    
    # Enhanced subtle styles to add after color replacements
    enhanced_subtle_styles = """
        /* Enhanced subtle theme styles */
        .glass-card {
            background: rgba(15, 23, 42, 0.75);
            backdrop-filter: blur(20px) saturate(120%);
            border: 1px solid rgba(100, 116, 139, 0.25);
        }

        .glass-card:hover {
            background: rgba(15, 23, 42, 0.85);
            border-color: rgba(100, 116, 139, 0.4);
            transform: translateY(-3px);
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

        .form-input {
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid rgba(100, 116, 139, 0.3);
        }

        .form-input:focus {
            background: rgba(15, 23, 42, 0.8);
            border-color: var(--primary-color);
            box-shadow: 0 0 0 3px rgba(100, 116, 139, 0.2);
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
                
                # Add enhanced styles if not already present
                if '</style>' in content and enhanced_subtle_styles not in content:
                    content = content.replace('</style>', f'{enhanced_subtle_styles}\n    </style>')
                
                # Only write if content actually changed
                if content != original_content:
                    with open(filepath, 'w', encoding='utf-8') as file:
                        file.write(content)
                    
                    updated_files.append(filename)
                    print(f"✅ Updated {filename} with subtle professional theme")
                else:
                    print(f"⏭️  No changes needed for {filename}")
                    
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")
    
    # Summary
    if updated_files:
        print(f"\n🎨 Successfully updated {len(updated_files)} file(s) to Subtle Professional Theme:")
        for file in updated_files:
            print(f"   • {file}")
        print(f"\n✨ Subtle theme features applied:")
        print(f"   • Soft charcoal backgrounds (#0f172a → #334155)")
        print(f"   • Muted slate blue accents (#64748b)")
        print(f"   • Subtle gray highlights (#94a3b8)")
        print(f"   • Reduced contrast for comfort")
        print(f"   • Professional, calming appearance")
    else:
        print("ℹ️  No files were updated. Templates may already use the subtle theme.")

if __name__ == "__main__":
    print("🎨 Subtle Professional Theme Updater")
    print("=" * 45)
    
    update_template_colors()
    
    print("\n💼 Professional, subtle theme applied!")
    print("🔄 Restart your FastAPI server to see the changes")
    print("✨ Much more calming and professional appearance")
