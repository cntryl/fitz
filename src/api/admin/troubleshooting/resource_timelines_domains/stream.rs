use super::super::{
    build_resource_timeline, matches_resource_path, stream_resource_diagnostics,
    timeline_candidate, DiagnosticSnapshot, ResourcePath, ResourceTimeline, ResourceTimelineEvent,
    ResourceTimelineKind,
};
use crate::api::admin::list::StreamInfo;
use chrono::Utc;

pub(crate) fn stream_resource_timeline(
    streams: &[StreamInfo],
    path: &ResourcePath<'_>,
    limit: usize,
) -> ResourceTimeline {
    let now = Utc::now();
    let stream = streams
        .iter()
        .find(|item| matches_resource_path(path, &item.realm, &item.area, &item.resource));
    let Some(stream) = stream else {
        return ResourceTimeline::new(
            "stream",
            path,
            None,
            DiagnosticSnapshot::healthy(),
            limit,
            Vec::new(),
        );
    };

    let diagnostics =
        stream_resource_diagnostics(stream.offset, stream.watermark, stream.sessions_active);
    let lag = stream.offset.saturating_sub(stream.watermark);
    let mut candidates = Vec::new();
    candidates.push(timeline_candidate(
        now,
        0,
        ResourceTimelineEvent::new(
            "stream",
            ResourceTimelineKind::Observation,
            now,
            if lag == 0 {
                format!(
                    "Stream is caught up with {} live append session(s)",
                    stream.sessions_active
                )
            } else {
                format!(
                    "Offset {}, watermark {}, lag {} event(s), {} live append session(s)",
                    stream.offset, stream.watermark, lag, stream.sessions_active
                )
            },
            path,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ));

    build_resource_timeline("stream", path, None, diagnostics, limit, candidates)
}
