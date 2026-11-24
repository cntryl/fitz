use super::model::{StreamDomainCommand, StreamFamilyRuntime};
use fitz_core::runtime::routing::RouteFamily;
use fitz_core::runtime::{DeliveryError, FamilyActorIngress, FamilyActorLane};

impl StreamFamilyRuntime {
    pub(super) fn run_command(&mut self, family: RouteFamily, command: StreamDomainCommand) {
        let is_delivery = matches!(command, StreamDomainCommand::Deliver(..));
        match command {
            StreamDomainCommand::ContinueDelivery => self.continuation_queued = false,
            StreamDomainCommand::Deliver(envelope, reply) => {
                let result = if *envelope.destination().family() == family {
                    self.deliver_envelope(&envelope)
                } else {
                    Err(DeliveryError::ActorStopped)
                };
                self.core.dispatch_ready_notifications();
                let _ = reply.send(result);
                // A continuously busy family never reaches the idle
                // handler, so service watermark coordination here as
                // well; otherwise commits queue up, overflow, and the
                // dropped ranges leave the area watermark stuck.
                self.service_watermark_timers();
            }
            StreamDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(self.core.live_counts());
            }
            StreamDomainCommand::ReadResourceRecords(command) => {
                let request = command.request.as_borrowed();
                let _ = command
                    .reply
                    .send(self.core.admin_read_resource_records(request));
            }
            StreamDomainCommand::CaptureStreamSnapshot(selector, reply) => {
                let _ = reply.send(self.core.capture_stream_snapshot(&selector));
            }
            StreamDomainCommand::RestoreStreamSnapshot(artifact, reply) => {
                let result = self.restore_stream_snapshot(&artifact);
                self.service_watermark_timers();
                let _ = reply.send(result);
            }
            StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                self.core.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            StreamDomainCommand::RunMaintenance {
                family: requested_family,
                reply,
            } => {
                if requested_family == family.as_u64() {
                    self.core.run_maintenance_slice(requested_family);
                    self.core.recheck_parked_slice();
                    self.service_watermark_timers();
                }
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            StreamDomainCommand::SyncAdminSnapshot(reply) => {
                self.core.sync_admin_snapshot();
                let _ = reply.send(());
            }
            StreamDomainCommand::PanicForFailpoint => {
                panic!("injected Stream family actor panic");
            }
            #[cfg(test)]
            StreamDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            #[cfg(test)]
            StreamDomainCommand::InspectForTests(inspect, reply) => {
                inspect(self);
                let _ = reply.send(());
            }
        }
        if !is_delivery {
            self.core.dispatch_ready_notifications();
        }
    }

    pub(super) fn offer_continuation(&mut self, ingress: &FamilyActorIngress<StreamDomainCommand>) {
        if !self.continuation_queued && self.core.delivery.has_ready() {
            self.continuation_queued = ingress
                .try_enqueue(
                    self.core.family,
                    FamilyActorLane::Normal,
                    StreamDomainCommand::ContinueDelivery,
                )
                .is_ok();
        }
    }
}
