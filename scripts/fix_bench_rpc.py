#!/usr/bin/env python3
"""Fix RpcRequest struct initializations in benchmark files by adding family_id field."""

import re
import sys

def fix_rpc_request(content: str) -> str:
    """Add family_id field to RpcRequest struct initializations."""
    # Pattern to match RpcRequest { without family_id
    # Captures the entire struct initialization
    pattern = r'(RpcRequest\s*\{)\s*\n(\s*)correlation_id:'
    
    # Replace with family_id as first field
    replacement = r'\1\n\2family_id: RouteFamily::new(1),\n\2correlation_id:'
    
    return re.sub(pattern, replacement, content)

def process_file(filepath: str):
    """Process a single file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    content = fix_rpc_request(content)
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✓ Fixed {filepath}")
        return 1
    else:
        print(f"  No changes needed in {filepath}")
        return 0

if __name__ == "__main__":
    files = [
        "benches/tier1_hotpath_rpc.rs",
        "benches/tier2_subsystem_rpc.rs",
    ]
    
    total = 0
    for filepath in files:
        total += process_file(filepath)
    
    print(f"\n{total} files updated")
