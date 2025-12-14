use std::fmt::{Debug, Error as FmtError, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;

use redb::Value;
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Portable, Serialize};

enum DbModelInner<'a, T> {
    Borrowed(&'a [u8], PhantomData<&'a T>),
    Owned(AlignedVec, PhantomData<&'a T>),
}

impl<'a, T> DbModelInner<'a, T>
where
    T: Portable + for<'b> CheckBytes<HighValidator<'b, RkyvError>>,
{
    pub fn new(bytes: &'a [u8]) -> Result<DbModelInner<'a, T>, RkyvError> {
        if bytes.as_ptr().align_offset(16) == 0 {
            rkyv::access::<T, RkyvError>(bytes)?;
            Ok(DbModelInner::Borrowed(bytes, PhantomData))
        } else {
            let mut vec = AlignedVec::with_capacity(bytes.len());
            vec.extend_from_slice(bytes);

            rkyv::access::<T, RkyvError>(&vec)?;
            Ok(DbModelInner::Owned(vec, PhantomData))
        }
    }
}

impl<'a, T> DbModelInner<'a, T> {
    pub fn get_bytes(&self) -> &[u8] {
        match self {
            DbModelInner::Borrowed(bytes, _) => &**bytes,
            DbModelInner::Owned(bytes, _) => &**bytes,
        }
    }
}

impl<'a, T> DbModelInner<'a, T>
where
    T: Portable,
{
    pub fn get(&self) -> &T {
        // SAFETY: all ways to arrive at bytes have already been checked by
        // `rkyv::access`
        unsafe { rkyv::access_unchecked(self.get_bytes()) }
    }
}

impl<'a, T> DbModelInner<'a, T> {
    pub fn into_owned(self) -> DbModelInner<'static, T> {
        match self {
            DbModelInner::Borrowed(bytes, _) => {
                let mut vec = AlignedVec::with_capacity(bytes.len());
                vec.extend_from_slice(bytes);

                DbModelInner::Owned(vec, PhantomData)
            }
            DbModelInner::Owned(bytes, _) => DbModelInner::Owned(bytes, PhantomData),
        }
    }
}

pub(crate) struct DbModel<'a, T: Archive> {
    inner: DbModelInner<'a, T::Archived>,
}

impl<'a, T> DbModel<'a, T>
where
    T: Archive,
    T::Archived: for<'b> CheckBytes<HighValidator<'b, RkyvError>>,
{
    pub(crate) fn new(bytes: &'a [u8]) -> Result<DbModel<'a, T>, RkyvError> {
        Ok(DbModel {
            inner: DbModelInner::new(bytes)?,
        })
    }
}

impl<'a, T> DbModel<'a, T>
where
    T: Archive,
{
    #[allow(unused)]
    pub(crate) fn into_owned(self) -> DbModel<'static, T> {
        DbModel {
            inner: self.inner.into_owned(),
        }
    }
}

impl<'a, T> DbModel<'a, T>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<RkyvError>>,
{
    pub(crate) fn to_unarchived(&self) -> Result<T, RkyvError> {
        rkyv::deserialize(self.inner.get())
    }
}

impl<'a, T> DbModel<'a, T>
where
    T: Archive + for<'b> Serialize<HighSerializer<AlignedVec, ArenaHandle<'b>, RkyvError>>,
{
    pub(crate) fn from_unarchived(this: &T) -> Self {
        let bytes = rkyv::to_bytes(this).unwrap();
        Self {
            inner: DbModelInner::Owned(bytes, PhantomData),
        }
    }
}

impl<'a, T> Deref for DbModel<'a, T>
where
    T: Archive,
{
    type Target = T::Archived;

    fn deref(&self) -> &Self::Target {
        self.inner.get()
    }
}

impl<'a, T> AsRef<[u8]> for DbModel<'a, T>
where
    T: Archive,
{
    fn as_ref(&self) -> &[u8] {
        self.inner.get_bytes()
    }
}

impl<'a, T> Debug for DbModel<'a, T>
where
    T: Archive,
    T::Archived: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        self.inner.get().fmt(f)
    }
}

impl<'a, T> Default for DbModel<'a, T>
where
    T: Archive
        + Default
        + for<'b> Serialize<HighSerializer<AlignedVec, ArenaHandle<'b>, RkyvError>>,
{
    fn default() -> Self {
        Self::from_unarchived(&T::default())
    }
}

impl<T> Value for DbModel<'_, T>
where
    T: Archive,
    T::Archived: Portable + for<'b> CheckBytes<HighValidator<'b, RkyvError>>,
{
    type SelfType<'a>
        = DbModel<'a, T>
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a DbModel<'a, T>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        DbModel::new(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value
    }
}
