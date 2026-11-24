//! Test-only generic actor adapter for direct Schedule actor tests.

use super::model::ScheduleActor;
use crate::domain::protocol::ScheduleMessage;
use fitz_core::runtime::actor::Context;
use fitz_core::runtime::Actor;

impl Actor for ScheduleActor {
    type Message = ScheduleMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = self.handle(msg);
        let _ = ctx.reply(response);
    }
}
