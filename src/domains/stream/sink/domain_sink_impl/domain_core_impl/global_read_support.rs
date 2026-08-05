use super::{route_triplet, Route, RouteFamily, StreamDomainCore};

impl StreamDomainCore {
    pub(in crate::domains::stream::sink) fn encode_last_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }
        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let data = actor
            .lock()
            .last()?
            .record
            .as_ref()
            .map(Self::encode_stream_last_data)
            .unwrap_or_default();
        Ok(data)
    }

    pub(in crate::domains::stream::sink) fn encode_metadata_response_data(
        &self,
        family_id: RouteFamily,
        route: &Route,
    ) -> Result<Vec<u8>, String> {
        let parts =
            route_triplet(route.as_str()).ok_or_else(|| "invalid stream route".to_string())?;
        if parts.area == "*" || parts.resource == "*" {
            return Ok(Vec::new());
        }
        let key = Self::actor_key_for_route(family_id, route)?;
        let actor = self.get_or_create_actor(&key)?;
        let metadata = actor.lock().metadata()?.metadata;
        Ok(Self::encode_stream_metadata_data(&metadata))
    }
}
