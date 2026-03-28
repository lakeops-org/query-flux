//! Reusable user-defined Python scripts (translation fixups, future: routing).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Row in `user_scripts`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct UserScriptRecord {
    pub id: i64,
    pub name: String,
    pub description: String,
    /// `translation_fixup` | `routing`
    pub kind: String,
    pub body: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Create or replace body of a script (`POST` full create, `PUT` update).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertUserScript {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// `translation_fixup` | `routing`
    pub kind: String,
    pub body: String,
}

pub const KIND_TRANSLATION_FIXUP: &str = "translation_fixup";
pub const KIND_ROUTING: &str = "routing";

pub fn is_valid_script_kind(kind: &str) -> bool {
    matches!(kind, KIND_TRANSLATION_FIXUP | KIND_ROUTING)
}
