//! Schedule definition CRUD: dispatching Create/CreateBatch/Cancel/List
//! messages to the per-family `ScheduleActor`, and hydrating those actors
//! from persisted storage at startup.

use super::model::{duration_millis, now_epoch_ms, ScheduleDomainRuntime, EXECUTIONS_WINDOW_MS};

impl ScheduleDomainRuntime<'_> {
    pub(super) fn apply_schedule_message(
        &mut self,
        actor: &mut crate::domains::schedule::ScheduleActor,
        schedule_msg: crate::domains::schedule::ScheduleMessage,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};

        match schedule_msg {
            ScheduleMessage::Create {
                route,
                cron,
                delivery_mode,
                payload,
            } => Self::apply_create_message(actor, route, cron, delivery_mode, payload),
            ScheduleMessage::CreateBatch { entries } => {
                Self::apply_create_batch_message(actor, entries)
            }
            ScheduleMessage::Cancel { route } => Self::apply_cancel_message(actor, &route),
            ScheduleMessage::List { offset, limit } => {
                let response = match actor.list_entries(offset, limit) {
                    Ok((entries, total_count)) => ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    },
                    Err(error) => {
                        ScheduleResponse::Error(crate::domains::schedule::ScheduleFailure::new(
                            crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                            error,
                        ))
                    }
                };
                (response, false)
            }
            ScheduleMessage::ListV2 { cursor, limit } => {
                let response = match actor.list_entries_v2(cursor.as_deref(), limit) {
                    Ok((entries, has_more, continuation)) => ScheduleResponse::ListPage {
                        entries,
                        has_more,
                        continuation,
                    },
                    Err(error) => {
                        ScheduleResponse::Error(crate::domains::schedule::ScheduleFailure::new(
                            crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                            error,
                        ))
                    }
                };
                (response, false)
            }
            ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => (
                self.apply_subscribe_message(family_id, &route, session_id, subscriber),
                false,
            ),
            ScheduleMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                ..
            } => (
                self.apply_unsubscribe_message(family_id, &route, session_id),
                false,
            ),
            ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                (ScheduleResponse::Ok, false)
            }
        }
    }

    fn apply_create_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        route: String,
        cron: String,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: bytes::Bytes,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleFailure, ScheduleResponse};

        if let Some(failure) =
            crate::domains::schedule::definition_validation::schedule_definition_failure(
                &route, &cron,
            )
        {
            return (ScheduleResponse::Error(failure), false);
        }

        match actor.create_schedule_with_mode(route, cron, delivery_mode, payload) {
            Ok(changed) => (ScheduleResponse::Ok, changed),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    fn apply_create_batch_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        entries: Vec<crate::domains::schedule::ScheduleCreateEntry>,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleFailure, ScheduleResponse};

        if let Some(failure) = entries.iter().find_map(|entry| {
            crate::domains::schedule::definition_validation::schedule_definition_failure(
                &entry.route,
                &entry.cron,
            )
        }) {
            return (ScheduleResponse::Error(failure), false);
        }

        match actor.create_schedules(entries) {
            Ok(changed) => (ScheduleResponse::Ok, changed > 0),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    fn apply_cancel_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        route: &str,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        if let Err(error) =
            crate::domains::schedule::protocol::validate_concrete_schedule_route(route)
        {
            return (
                ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::InvalidTarget,
                    error,
                )),
                false,
            );
        }

        match actor.delete_schedule(route) {
            Ok(removed) => (ScheduleResponse::Ok, removed),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    pub(super) fn get_or_create_actor<'a>(
        &self,
        actor: &'a mut Option<crate::domains::schedule::ScheduleActor>,
    ) -> Result<&'a mut crate::domains::schedule::ScheduleActor, String> {
        if actor.is_none() {
            *actor = Some(crate::domains::schedule::ScheduleActor::try_new(
                self.core.route_family,
                self.core.store.clone(),
                self.core.write_policy,
            )?);
        }
        Ok(actor.as_mut().expect("Schedule actor was initialized"))
    }

    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub(super) fn preload_persisted_families(&mut self) -> Result<(), String> {
        let started_at = std::time::Instant::now();
        let family = self.core.route_family;
        let persisted = self
            .core
            .store
            .has_family(family)
            .map_err(|error| error.to_string())?;
        let actor = &mut self.core.actor;
        if persisted && actor.is_none() {
            *actor = Some(crate::domains::schedule::ScheduleActor::try_new(
                family,
                self.core.store.clone(),
                self.core.write_policy,
            )?);
        }

        // Seed the rolling-window acknowledgement counter from persisted
        // last_fire_ms values so executions-per-minute survives restarts for
        // occurrences already acknowledged within the last 60 seconds.
        let now_ms = now_epoch_ms();
        let cutoff_ms = now_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        let deque = &mut self.core.recent_acknowledgement_ms;
        if let Some(actor) = actor.as_ref() {
            for ts in actor.last_fire_timestamps_since(cutoff_ms) {
                deque.push_back(ts);
            }
        }
        deque.make_contiguous().sort_unstable();
        self.schedule_admin_snapshot(true);
        tracing::info!(
            domain = "schedule",
            route_family = family.id(),
            persisted,
            elapsed_ms = duration_millis(started_at.elapsed()),
            "Schedule family projection preload completed"
        );
        Ok(())
    }
}
