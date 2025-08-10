#!/usr/bin/env python3
"""
Post-process Criterion HTML reports to include flamegraph links.

This script searches for Criterion HTML reports and adds flamegraph links
when flamegraph.svg files exist in the corresponding profile directories.
"""

import os
import re
from pathlib import Path

def add_flamegraph_link(html_content, flamegraph_path):
    """Add flamegraph link to HTML content."""
    # Find the "Additional Plots" section
    plots_section = r'(<h4>Additional Plots:</h4>\s*<ul>)'
    flamegraph_link = r'\1\n                        <li>\n                            <a href="../profile/flamegraph.svg">🔥 Flamegraph</a>\n                        </li>'
    
    # Add the flamegraph link at the beginning of the list
    return re.sub(plots_section, flamegraph_link, html_content)

def process_criterion_reports(criterion_dir):
    """Process all Criterion HTML reports to add flamegraph links."""
    criterion_path = Path(criterion_dir)
    
    if not criterion_path.exists():
        print(f"Criterion directory not found: {criterion_dir}")
        return
    
    reports_updated = 0
    
    # Find all index.html files in the criterion directory
    for html_file in criterion_path.rglob("index.html"):
        # Check if this is a benchmark report (not the main index)
        if html_file.parent.name == "report":
            # Look for corresponding flamegraph
            benchmark_dir = html_file.parent.parent
            flamegraph_file = benchmark_dir / "profile" / "flamegraph.svg"
            
            if flamegraph_file.exists():
                print(f"Adding flamegraph link to: {html_file.relative_to(criterion_path)}")
                
                # Read HTML content
                with open(html_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Check if flamegraph link already exists
                if "flamegraph.svg" not in content:
                    # Add flamegraph link
                    updated_content = add_flamegraph_link(content, flamegraph_file)
                    
                    # Write back to file
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(updated_content)
                    
                    reports_updated += 1
                    print(f"  ✅ Updated {html_file.name}")
                else:
                    print(f"  ⚠️  Flamegraph link already exists in {html_file.name}")
            else:
                print(f"  ❌ No flamegraph found for {benchmark_dir.name}")
    
    print(f"\nSummary: Updated {reports_updated} HTML reports with flamegraph links.")

def main():
    """Main function."""
    import sys
    
    # Default to target/criterion if no argument provided
    criterion_dir = sys.argv[1] if len(sys.argv) > 1 else "target/criterion"
    
    print("🔥 Adding flamegraph links to Criterion HTML reports...")
    print(f"Processing directory: {criterion_dir}")
    print("-" * 50)
    
    process_criterion_reports(criterion_dir)
    
    print("\n✅ Done! You can now view enhanced HTML reports with flamegraph links.")
    print("\nTo view reports:")
    print(f"  python -m http.server 8000 --directory {criterion_dir}")
    print("  Then open: http://localhost:8000/report/")

if __name__ == "__main__":
    main()