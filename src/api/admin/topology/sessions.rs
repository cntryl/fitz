use crate::api::admin::list::SessionInfo;
use std::collections::{BTreeMap, BTreeSet};

use super::types::TopologySessionGroup;

const REPRESENTATIVE_SESSION_LIMIT: usize = 5;

pub(super) fn session_groups(sessions: Vec<SessionInfo>) -> Vec<TopologySessionGroup> {
    let mut grouped: BTreeMap<u64, Vec<SessionInfo>> = BTreeMap::new();
    for session in sessions {
        grouped
            .entry(session.route_family)
            .or_default()
            .push(session);
    }

    grouped
        .into_iter()
        .map(|(route_family, mut sessions)| {
            sessions.sort_by(|left, right| {
                let left_volume = left.messages_received + left.messages_sent;
                let right_volume = right.messages_received + right.messages_sent;
                right_volume
                    .cmp(&left_volume)
                    .then_with(|| left.session_id.cmp(&right.session_id))
            });

            let transports = sessions
                .iter()
                .filter(|session| !session.transport.is_empty())
                .map(|session| session.transport.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let messages_received = sessions
                .iter()
                .map(|session| session.messages_received)
                .sum();
            let messages_sent = sessions.iter().map(|session| session.messages_sent).sum();
            let max_idle_seconds = sessions
                .iter()
                .map(|session| session.idle_seconds)
                .max()
                .unwrap_or_default();

            TopologySessionGroup {
                route_family,
                sessions: sessions.len(),
                messages_received,
                messages_sent,
                transports,
                max_idle_seconds,
                representative_sessions: sessions
                    .into_iter()
                    .take(REPRESENTATIVE_SESSION_LIMIT)
                    .collect(),
            }
        })
        .collect()
}
