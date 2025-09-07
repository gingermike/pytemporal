#!/usr/bin/env python3
"""
Add PyTemporal branding to Criterion HTML reports.

This script enhances Criterion HTML reports with:
- PyTemporal logo in header
- Custom CSS styling with brand colors
- Branded page titles and footer
- Consistent visual identity
"""

import os
import re
from pathlib import Path

# PyTemporal logo SVG (inline for embedding)
PYTEMPORAL_LOGO = '''<svg width="280" height="80" viewBox="0 0 280 80" xmlns="http://www.w3.org/2000/svg" style="margin-right: 20px;">
  <!-- Layered/stacked design suggesting temporal dimensions -->
  <!-- Bottom layer (as-of time) -->
  <rect x="15" y="45" width="40" height="8" fill="#94a3b8" rx="4"/>
  <!-- Middle layer (intersection) -->
  <rect x="20" y="35" width="40" height="8" fill="#3b82f6" rx="4"/>
  <!-- Top layer (effective time) -->
  <rect x="25" y="25" width="40" height="8" fill="#1d4ed8" rx="4"/>
  <!-- Typography -->
  <text x="75" y="40" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" 
        font-size="24" font-weight="600" fill="#1f2937" dominant-baseline="middle">PyTemporal</text>
</svg>'''

# Enhanced CSS with PyTemporal branding
ENHANCED_CSS = """
        /* PyTemporal Branding Enhancements */
        .pytemporal-header {
            display: flex;
            align-items: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 3px solid #3b82f6;
        }
        
        .pytemporal-title {
            color: #1f2937;
            font-weight: 300;
            margin: 0;
        }
        
        /* Enhanced color scheme */
        body {
            font: 14px -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            text-rendering: optimizelegibility;
            color: #1f2937;
            line-height: 1.6;
        }

        h2, h3, h4 {
            color: #1f2937;
        }
        
        a:link {
            color: #3b82f6;
            text-decoration: none;
        }
        
        a:hover {
            color: #1d4ed8;
            text-decoration: underline;
        }
        
        /* Enhanced flamegraph link styling */
        .additional_plots a[href*="flamegraph"] {
            background: linear-gradient(135deg, #f59e0b, #d97706);
            color: white;
            padding: 4px 8px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: 500;
        }
        
        .additional_plots a[href*="flamegraph"]:hover {
            background: linear-gradient(135deg, #d97706, #b45309);
            text-decoration: none;
        }
        
        /* Enhanced footer */
        #footer {
            height: 50px;
            background: linear-gradient(135deg, #1d4ed8, #3b82f6);
            color: white;
            font-size: 14px;
            font-weight: 400;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        /* Improved table styling */
        table {
            border-collapse: collapse;
            margin: 10px 0;
        }
        
        th, td {
            padding: 8px 12px;
            border: 1px solid #e5e7eb;
        }
        
        th {
            background: #f9fafb;
            font-weight: 500;
            color: #374151;
        }
"""

def add_pytemporal_branding(html_content):
    """Add PyTemporal branding to HTML content."""
    
    # Replace page title
    html_content = re.sub(
        r'<title>(.*?) - Criterion\.rs</title>',
        r'<title>\1 - PyTemporal Benchmarks</title>',
        html_content
    )
    
    # Replace main index title
    html_content = re.sub(
        r'<title>Index - Criterion\.rs</title>',
        r'<title>PyTemporal Benchmarks - Performance Reports</title>',
        html_content
    )
    
    # Add enhanced CSS after existing styles
    css_insertion_point = r'(</style>)'
    enhanced_css_block = ENHANCED_CSS + r'\1'
    html_content = re.sub(css_insertion_point, enhanced_css_block, html_content)
    
    # Replace h2 headers with branded header
    # For individual benchmark pages
    html_content = re.sub(
        r'<h2>([^<]+)</h2>',
        lambda m: f'''<div class="pytemporal-header">
            {PYTEMPORAL_LOGO}
            <div>
                <h2 class="pytemporal-title">{m.group(1)}</h2>
                <p style="margin: 5px 0 0 0; color: #6b7280; font-size: 16px;">High-Performance Bitemporal Processing Benchmarks</p>
            </div>
        </div>''',
        html_content
    )
    
    # For main index page
    html_content = re.sub(
        r'<h2>Criterion\.rs Benchmark Index</h2>',
        f'''<div class="pytemporal-header">
            {PYTEMPORAL_LOGO}
            <div>
                <h2 class="pytemporal-title">PyTemporal Performance Benchmarks</h2>
                <p style="margin: 5px 0 0 0; color: #6b7280; font-size: 16px;">High-Performance Bitemporal Processing Results</p>
            </div>
        </div>''',
        html_content
    )
    
    # Update footer with PyTemporal branding
    footer_replacement = '''<div id="footer">
        <p>Performance benchmarks generated by <a href="https://github.com/bheisler/criterion.rs">Criterion.rs</a> for 
           <strong>PyTemporal</strong> - High-Performance Bitemporal Processing Library</p>
    </div>'''
    
    html_content = re.sub(
        r'<div id="footer">.*?</div>',
        footer_replacement,
        html_content,
        flags=re.DOTALL
    )
    
    return html_content

def process_html_reports(criterion_dir):
    """Process all HTML reports to add PyTemporal branding."""
    criterion_path = Path(criterion_dir)
    
    if not criterion_path.exists():
        print(f"Criterion directory not found: {criterion_dir}")
        return
    
    reports_updated = 0
    
    # Process all index.html files
    for html_file in criterion_path.rglob("index.html"):
        print(f"Adding PyTemporal branding to: {html_file.relative_to(criterion_path)}")
        
        # Read HTML content
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Add branding
        branded_content = add_pytemporal_branding(content)
        
        # Write back to file
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(branded_content)
        
        reports_updated += 1
        print(f"  ✅ Updated {html_file.name}")
    
    print(f"\nSummary: Added PyTemporal branding to {reports_updated} HTML reports.")

def main():
    """Main function."""
    import sys
    
    # Default to target/criterion if no argument provided
    criterion_dir = sys.argv[1] if len(sys.argv) > 1 else "target/criterion"
    
    print("🎨 Adding PyTemporal branding to Criterion HTML reports...")
    print(f"Processing directory: {criterion_dir}")
    print("-" * 50)
    
    process_html_reports(criterion_dir)
    
    print("\n✅ Done! HTML reports now feature PyTemporal branding.")
    print("\nTo view branded reports:")
    print(f"  python -m http.server 8000 --directory {criterion_dir}")
    print("  Then open: http://localhost:8000/report/")

if __name__ == "__main__":
    main()