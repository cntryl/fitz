//! Route path parsing: scheme://realm/area/resource/operation

#[derive(Debug, Clone)]
pub struct RoutePath {
    pub scheme: String,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
}

impl RoutePath {
    pub fn parse(_s: &str) -> Result<Self, String> {
        // TODO: Implement parser
        todo!()
    }
}
