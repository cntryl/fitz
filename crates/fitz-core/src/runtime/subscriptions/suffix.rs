//! Rolling frontiers for the suffix after an indexed `**` segment.

use super::{CompiledPatternSegment, CompiledRouteSegment};
use smallvec::SmallVec;

/// The implicit leading `**` makes every empty-pattern state reachable.
/// Matching stays polynomial in suffix and route length. Ordinary routes keep
/// both frontiers inline; longer routes allocate each buffer just once.
#[inline]
pub(super) fn matches_suffix_compiled(
    suffix: &[CompiledPatternSegment],
    route: &[CompiledRouteSegment],
    start_idx: usize,
) -> bool {
    let route = route.get(start_idx..).unwrap_or_default();
    let mut initial = SmallVec::<[bool; 9]>::from_elem(true, route.len() + 1);
    let mut scratch = SmallVec::<[bool; 9]>::from_elem(false, route.len() + 1);
    // Swap slice views, so swapping frontiers never moves the inline buffers.
    let mut previous = initial.as_mut_slice();
    let mut current = scratch.as_mut_slice();

    for pattern in suffix {
        current.fill(false);
        match pattern {
            CompiledPatternSegment::DoubleStar => {
                current[0] = previous[0];
                for index in 1..=route.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            CompiledPatternSegment::Star => {
                current[1..].copy_from_slice(&previous[..route.len()]);
            }
            CompiledPatternSegment::Exact(expected_id) => {
                for index in 1..=route.len() {
                    current[index] =
                        previous[index - 1] && route[index - 1].exact_id == Some(*expected_id);
                }
            }
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[route.len()]
}
