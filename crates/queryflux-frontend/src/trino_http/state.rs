// AppState lives at the frontend crate root so it can be shared across
// all frontend protocol implementations (Trino HTTP, PG wire, etc.).
pub use crate::state::AppState;
