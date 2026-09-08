//! Test-only generic actor adapter for direct Schedule actor tests.

use super::model::ScheduleActor;
use crate::domains::schedule::protocol::ScheduleMessage;
use crate::prelude::Actor;
use crate::runtime::actor::Context;

impl Actor for ScheduleActor {
    type Message = ScheduleMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = self.handle(msg);
        let _ = ctx.reply(response);
    }
}
