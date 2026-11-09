/// Sign of a row change in an incremental scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RowKind {
    Negative = -1,
    Positive = 1,
}

impl RowKind {
    pub const fn code(self) -> i32 {
        self as i32
    }
}
