#!/usr/bin/env python3
"""
Script to add family_id field to all RpcRequest initializations in test files.
Adds 'family_id: RouteFamily::new(X),' as the first field.
"""

import re
import sys
from pathlib import Path

def update_rpc_request(content, default_family=1):
    """Add family_id to RpcRequest struct initializations"""
    
    # Pattern to match RpcRequest { ... }
    # We look for the pattern and add family_id after the opening brace
    pattern = r'(RpcRequest\s*\{)\s*\n(\s*)(correlation_id:)'
    
    def replacement(match):
        opening = match.group(1)
        indent = match.group(2)
        correlation = match.group(3)
        
        # Determine family number from context if possible
        # For now, use default
        return f'{opening}\n{indent}family_id: RouteFamily::new({default_family}),\n{indent}{correlation}'
    
    updated = re.sub(pattern, replacement, content)
    return updated

def process_file(filepath):
    """Process a single file"""
    print(f"Processing {filepath}...")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if file needs updating
    if 'RpcRequest {' not in content or 'family_id: RouteFamily::new' in content:
        print(f"  Skipping {filepath} (no changes needed or already updated)")
        return False
    
    # Determine family number from file context
    family = 1
    if 'RouteFamily::new(2)' in content:
        # File has family 2, we might need special handling
        pass
    
    updated = update_rpc_request(content, family)
    
    if updated != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(updated)
        print(f"  Updated {filepath}")
        return True
    else:
        print(f"  No changes in {filepath}")
        return False

def main():
    # Find all RPC test files
    test_files = [
        Path('tests/rpc_semantics.rs'),
        Path('tests/rpc_e2e_basic.rs'),
        Path('tests/rpc_auth.rs'),
        Path('tests/rpc_lease_fault_tolerance.rs'),
        Path('tests/rpc_streaming_ordering.rs'),
        Path('src/domains/rpc/session.rs'),
    ]
    
    updated_count = 0
    for filepath in test_files:
        if filepath.exists():
            if process_file(filepath):
                updated_count += 1
        else:
            print(f"Warning: {filepath} not found")
    
    print(f"\nUpdated {updated_count} files")

if __name__ == '__main__':
    main()
