use super::*;

// TODO
// TxBegin(TxID), // in-mem
// TxCommit(TxID), // in-mem
// TxAbort(TxID), // in-mem

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Frag {
    Set(IVec, IVec),
    Del(IVec),
    Merge(IVec, IVec),
    Base(Node),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
    Counter(usize),
    Meta(Meta),
    RightMerge,
    LeftMerge(LeftMerge),
    ParentMerge(PageId),
}

impl Frag {
    pub(super) fn unwrap_base(&self) -> &Node {
        if let Frag::Base(base, ..) = self {
            base
        } else {
            panic!("called unwrap_base_ptr on non-Base Frag!")
        }
    }

    pub(super) fn unwrap_meta(&self) -> &Meta {
        if let Frag::Meta(meta) = self {
            meta
        } else {
            panic!("called unwrap_base_ptr on non-Base Frag!")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ParentSplit {
    pub(crate) at: IVec,
    pub(crate) to: PageId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChildSplit {
    pub(crate) at: IVec,
    pub(crate) to: PageId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct LeftMerge {
    pub(crate) new_hi: IVec,
    pub(crate) new_next: Option<PageId>,
    pub(crate) merged_items: Vec<(IVec, IVec)>,
}
