#!/usr/bin/env python3
"""Update all LeaseActor::new() to LeaseActor::new(RouteFamily::new(1))"""

from pathlib import Path

def update_file(filepath):
    """Update LeaseActor::new() calls in a file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if 'LeaseActor::new()' not in content:
        return False
    
    updated = content.replace('LeaseActor::new()', 'LeaseActor::new(RouteFamily::new(1))')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(updated)
    
    print(f"Updated {filepath}")
    return True

def main():
    files = [
        # Tests
        'tests/lease_semantics.rs',
        'tests/lease_e2e_basic.rs',
        'tests/lease_auth.rs',
        # Source
        'src/domains/lease/lease_actor.rs',
        'src/domains/lease/session.rs',
        'src/domains/lease/guard.rs',
        'src/domains/lease/mod.rs',
        # Benches
        'benches/tier2_subsystem_lease.rs',
    ]
    
    updated = 0
    for f in files:
        p = Path(f)
        if p.exists():
            if update_file(p):
                updated += 1
        else:
            print(f"Not found: {f}")
    
    print(f"\nUpdated {updated} files")

if __name__ == '__main__':
    main()
