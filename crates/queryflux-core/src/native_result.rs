use bytes::Bytes;

/// Describes one column in a native (non-Arrow) result set.
///
/// Lives in `queryflux-core` with zero driver imports — adapters convert their
/// driver-specific column metadata *into* this; frontends read *from* this to
/// build wire-protocol column definition packets.
#[derive(Debug, Clone)]
pub struct NativeColumn {
    pub name: String,
    pub type_info: NativeTypeInfo,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct NativeTypeInfo {
    pub kind: NativeTypeKind,
    pub precision: Option<u16>,
    pub scale: Option<u8>,
    pub unsigned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeTypeKind {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Char,
    Varchar,
    Text,
    Binary,
    Blob,
    Date,
    Time,
    DateTime,
    Timestamp,
    Json,
    Unknown,
}

/// A single result row: one nullable, text-encoded value per column.
///
/// `None` = SQL NULL. `Some(bytes)` = the value's display representation (UTF-8).
/// Pre-encoding at the adapter avoids per-value string formatting in the frontend.
/// `Bytes` cloning is O(1) (reference-counted).
#[derive(Debug, Clone)]
pub struct NativeRow(pub Vec<Option<Bytes>>);

/// A batch of rows from a native result stream.
///
/// `columns` is `Some` only on the first chunk — the schema is delivered once,
/// then subsequent chunks carry rows only.
#[derive(Debug, Clone)]
pub struct NativeResultChunk {
    /// Column metadata. Present on the first chunk, `None` on all subsequent chunks.
    pub columns: Option<Vec<NativeColumn>>,
    pub rows: Vec<NativeRow>,
}
