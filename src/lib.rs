use std::ops::Deref;

use abi_stable::std_types::{RHashMap, RString, RVec, Tuple2};
use bincode::{
    Decode, Encode,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use sled::{Batch, Db, Tree};
use steel::{
    rvals::Custom,
    steel_vm::ffi::{
        CustomRef, FFIArg, FFIModule, FFIValue, RegisterFFIFn, as_underlying_ffi_type,
    },
};

steel::declare_module!(build_module);

pub fn build_module() -> FFIModule {
    sled_module()
}

#[derive(Copy, Clone, Debug)]
enum KeyType {
    Integer,
    String,
    Bytes,
    Any,
}

enum DbOrTree {
    Db(Db<1024>),
    Tree(Tree<1024>),
}

impl AsRef<Tree> for DbOrTree {
    fn as_ref(&self) -> &Tree {
        match self {
            DbOrTree::Db(db) => &db,
            DbOrTree::Tree(tree) => tree,
        }
    }
}

impl Deref for DbOrTree {
    type Target = Tree<1024>;
    fn deref(&self) -> &Tree {
        match self {
            DbOrTree::Db(db) => &db,
            DbOrTree::Tree(tree) => tree,
        }
    }
}

// Values have to be encoded correctly, values are encoded
// more or less directly as the enum values unless otherwise specified.
struct SledDb {
    db: DbOrTree,
    key_type: KeyType,
}

#[derive(Debug)]
enum SledError {
    Io(std::io::Error),
    Encoding(EncodeError),
    CannotOpenTreeFromTree,
    UnknownKeyType(String),
    TypeMismatch(String),
}

impl Custom for SledError {}

impl std::fmt::Display for SledError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SledError::Io(error) => write!(f, "{}", error),
            SledError::Encoding(encode_error) => write!(f, "{}", encode_error),
            Self::CannotOpenTreeFromTree => write!(f, "CannotOpenTreeFromTree"),
            Self::UnknownKeyType(key) => write!(f, "UnknownKeyType({})", key),
            Self::TypeMismatch(s) => write!(f, "TypeMismatch: {}", s),
        }
    }
}

impl From<EncodeError> for SledError {
    fn from(value: EncodeError) -> Self {
        Self::Encoding(value)
    }
}

impl From<std::io::Error> for SledError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::error::Error for SledError {}

// We're good here since the steel side
// is going to be sync
unsafe impl Sync for SledDb {}

impl Custom for SledDb {}

struct BincodeWrapper {
    value: FFIValue,
}

// Encode directly to a FFIValue!
struct ArgBincodeWrapper<'a> {
    value: FFIArg<'a>,
}

impl<'a> Encode for ArgBincodeWrapper<'a> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Manually do our recursion here
        fn encode_inner<EI: bincode::enc::Encoder>(
            this: &FFIArg,
            encoder: &mut EI,
        ) -> Result<(), bincode::error::EncodeError> {
            match this {
                FFIArg::BoolV(b) => {
                    bincode::Encode::encode(&0u32, encoder)?;
                    bincode::Encode::encode(b, encoder)?
                }
                FFIArg::NumV(n) => {
                    bincode::Encode::encode(&1u32, encoder)?;
                    bincode::Encode::encode(n, encoder)?
                }
                FFIArg::IntV(i) => {
                    bincode::Encode::encode(&2u32, encoder)?;
                    bincode::Encode::encode(i, encoder)?
                }
                FFIArg::Void => {
                    bincode::Encode::encode(&3u32, encoder)?;
                    bincode::Encode::encode("#<void>", encoder)?
                }
                FFIArg::StringV(rstring) => {
                    bincode::Encode::encode(&4u32, encoder)?;
                    bincode::Encode::encode(rstring.as_str(), encoder)?
                }
                FFIArg::Vector(rvec) => {
                    bincode::Encode::encode(&5u32, encoder)?;
                    encode_slice_len(encoder, rvec.len())?;
                    for value in rvec {
                        encode_inner(value, encoder)?;
                    }
                }

                FFIArg::VectorRef(rvec) => {
                    bincode::Encode::encode(&5u32, encoder)?;
                    encode_slice_len(encoder, rvec.vec.len())?;
                    for value in rvec.vec.iter() {
                        encode_value_inner(value, encoder)?;
                    }
                }

                FFIArg::CharV { c } => {
                    bincode::Encode::encode(&6u32, encoder)?;
                    bincode::Encode::encode(c, encoder)?
                }
                FFIArg::HashMap(rhash_map) => {
                    bincode::Encode::encode(&7u32, encoder)?;
                    encode_slice_len(encoder, rhash_map.len())?;

                    // Encode the hash map just as a series
                    // of key value pairs.
                    for Tuple2(key, value) in rhash_map {
                        encode_inner(key, encoder)?;
                        encode_inner(value, encoder)?;
                    }
                }
                FFIArg::ByteVector(rvec) => {
                    bincode::Encode::encode(&8u32, encoder)?;
                    bincode::Encode::encode(rvec.as_slice(), encoder)?;
                    for value in rvec {
                        bincode::Encode::encode(value, encoder)?;
                    }
                }
                _ => {
                    return Err(EncodeError::OtherString(format!(
                        "Value not encodable: {:?}",
                        this
                    )));
                }
            }

            Ok(())
        }

        encode_inner(&self.value, encoder)
    }
}

pub(crate) fn encode_slice_len<E: Encoder>(encoder: &mut E, len: usize) -> Result<(), EncodeError> {
    (len as u64).encode(encoder)
}

// Manually do our recursion here
fn encode_value_inner<EI: bincode::enc::Encoder>(
    this: &FFIValue,
    encoder: &mut EI,
) -> Result<(), bincode::error::EncodeError> {
    match this {
        FFIValue::BoolV(b) => {
            bincode::Encode::encode(&0u32, encoder)?;
            bincode::Encode::encode(b, encoder)?
        }
        FFIValue::NumV(n) => {
            bincode::Encode::encode(&1u32, encoder)?;
            bincode::Encode::encode(n, encoder)?
        }
        FFIValue::IntV(i) => {
            bincode::Encode::encode(&2u32, encoder)?;
            bincode::Encode::encode(i, encoder)?
        }
        FFIValue::Void => {
            bincode::Encode::encode(&3u32, encoder)?;
            bincode::Encode::encode("#<void>", encoder)?
        }
        FFIValue::StringV(rstring) => {
            bincode::Encode::encode(&4u32, encoder)?;
            bincode::Encode::encode(rstring.as_str(), encoder)?
        }
        FFIValue::Vector(rvec) => {
            bincode::Encode::encode(&5u32, encoder)?;
            encode_slice_len(encoder, rvec.len())?;
            for value in rvec {
                encode_value_inner(value, encoder)?;
            }
        }
        FFIValue::CharV { c } => {
            bincode::Encode::encode(&6u32, encoder)?;
            bincode::Encode::encode(c, encoder)?
        }
        FFIValue::HashMap(rhash_map) => {
            bincode::Encode::encode(&7u32, encoder)?;
            encode_slice_len(encoder, rhash_map.len())?;

            // Encode the hash map just as a series
            // of key value pairs.
            for Tuple2(key, value) in rhash_map {
                encode_value_inner(key, encoder)?;
                encode_value_inner(value, encoder)?;
            }
        }
        FFIValue::ByteVector(rvec) => {
            bincode::Encode::encode(&8u32, encoder)?;
            bincode::Encode::encode(rvec.as_slice(), encoder)?;
            for value in rvec {
                bincode::Encode::encode(value, encoder)?;
            }
        }
        _ => {
            return Err(EncodeError::OtherString(format!(
                "Value not encodable: {:?}",
                this
            )));
        }
    }

    Ok(())
}

impl Encode for BincodeWrapper {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        encode_value_inner(&self.value, encoder)
    }
}

// Manually implement a stack for deserializing the values
// from bytes. That way the values can be properly deserialized
#[derive(Copy, Clone)]
enum ValueKind {
    Vec,
    Map,
}

fn vec_to_map(vec: RVec<FFIValue>) -> RHashMap<FFIValue, FFIValue> {
    let mut map = RHashMap::with_capacity(vec.len());
    let mut vec = vec.into_iter();
    while let Some(key) = vec.next() {
        let value = vec.next().unwrap();
        map.insert(key, value);
    }

    map
}

// TODO: Figure this out
impl<Context> Decode<Context> for BincodeWrapper {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Recur on the children?
        let mut stack: Vec<(usize, ValueKind, RVec<FFIValue>)> = Vec::new();

        macro_rules! maybe_return {
            ($value:expr) => {{
                let mut last = $value;
                while let Some(guard) = stack.last_mut() {
                    guard.0 -= 1;
                    guard.2.push(last);

                    let kind = guard.1;

                    if guard.0 == 0 {
                        // We've hit the end of our decoding, we should be okay to
                        // do the thing.
                        let value_vec = stack.pop().unwrap();

                        match kind {
                            ValueKind::Vec => {
                                let value = FFIValue::Vector(value_vec.2);
                                if stack.is_empty() {
                                    return Ok(BincodeWrapper { value });
                                } else {
                                    last = value;
                                }
                            }

                            ValueKind::Map => {
                                let value = FFIValue::HashMap(vec_to_map(value_vec.2));
                                if stack.is_empty() {
                                    return Ok(BincodeWrapper { value });
                                } else {
                                    last = value;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }};
        }

        loop {
            let header = u32::decode(decoder)?;

            match header {
                // Bool
                0 => {
                    let bool = bool::decode(decoder)?;
                    let value = FFIValue::BoolV(bool);

                    if stack.is_empty() {
                        return Ok(BincodeWrapper { value });
                    }

                    maybe_return!(value);
                }
                // NumV
                1 => {
                    let number = f64::decode(decoder)?;
                    let value = FFIValue::NumV(number);

                    if stack.is_empty() {
                        return Ok(BincodeWrapper { value });
                    }

                    maybe_return!(value);
                }
                // Int
                2 => {
                    let number = isize::decode(decoder)?;
                    let value = FFIValue::IntV(number);

                    if stack.is_empty() {
                        return Ok(BincodeWrapper { value });
                    }
                    maybe_return!(value);
                }
                // Void
                3 => {
                    let void = String::decode(decoder)?;
                    if void == "#<void>" {
                        return Ok(BincodeWrapper {
                            value: FFIValue::Void,
                        });
                    }

                    if stack.is_empty() {
                        return Ok(BincodeWrapper {
                            value: FFIValue::Void,
                        });
                    }

                    maybe_return!(FFIValue::Void);
                }
                // String
                4 => {
                    let string = String::decode(decoder)?;
                    let value = FFIValue::StringV(RString::from(string));
                    if stack.is_empty() {
                        return Ok(BincodeWrapper { value });
                    }
                    maybe_return!(value);
                }
                // Vector
                5 => {
                    // Push on the size, alongside a vector with the capacity
                    // that we want.
                    let size = u64::decode(decoder)?;

                    // Now, we'll start pushing values on.
                    // Once those values are done, we'll pop it off, and otherwise
                    // push it in to the remaining stack
                    stack.push((size as _, ValueKind::Vec, RVec::with_capacity(size as _)));
                }


                // Char
                6 => {
                    let c = char::decode(decoder)?;
                    let value = FFIValue::CharV { c };
                    if stack.is_empty() {
                        return Ok(BincodeWrapper { value });
                    }
                    maybe_return!(value);
                }
                7 => {
                    // Similar to vector here, we have encoded how long the map is.
                    // Realistically we should branch and have a separate allocation type
                    // for this, but that is more difficult since we'll have to
                    // decide for keys vs values.
                    let size = u64::decode(decoder)?;

                    stack.push((size as _, ValueKind::Map, RVec::with_capacity(size as usize * 2)));
                }
                ,
                // Bytevector - this _should_ be pretty easy?
                8 => {
                    let length = u64::decode(decoder)?;
                    let mut bytevector = RVec::with_capacity(length as _);

                    for _ in 0..length {
                        bytevector.push(u8::decode(decoder)?);
                    }

                    if stack.is_empty() {
                        return Ok(BincodeWrapper {
                            value: FFIValue::ByteVector(bytevector),
                        });
                    }

                    maybe_return!(FFIValue::ByteVector(bytevector));
                }
                _ => {
                    return Err(DecodeError::OtherString(format!(
                        "Unable to deserialize ffi value"
                    )));
                }
            }
        }
    }
}

// TODO: Provide a configuration for sharing the buffer
pub fn encode_value(value: FFIValue) -> Result<Vec<u8>, EncodeError> {
    let val = BincodeWrapper { value };
    bincode::encode_to_vec(val, bincode::config::standard())
}

pub fn encode_arg(value: FFIArg) -> Result<Vec<u8>, EncodeError> {
    let val = ArgBincodeWrapper { value };
    bincode::encode_to_vec(val, bincode::config::standard())
}

pub fn decode_value(bytes: &[u8]) -> FFIValue {
    let value: BincodeWrapper = bincode::decode_from_slice(bytes, bincode::config::standard())
        .unwrap()
        .0;

    value.value
}

#[test]
pub fn test_basic() {
    // let value = FFIValue::IntV(10);

    let value = FFIValue::Vector(
        vec![
            FFIValue::IntV(10),
            FFIValue::Vector(vec![FFIValue::IntV(20)].into()),
            FFIValue::IntV(30),
        ]
        .into(),
    );

    let encoded = encode_value(value);
    let decoded = decode_value(encoded.unwrap().as_slice());

    dbg!(decoded);
}

// Set up batches of changes to be applied atomically
struct SledBatch(Batch);

impl Custom for SledBatch {}

impl SledBatch {
    pub fn new() -> Self {
        Self(Batch::default())
    }

    pub fn insert(&mut self, key: FFIArg, value: FFIArg) -> Result<(), SledError> {
        self.0.insert(encode_arg(key)?, encode_arg(value)?);
        Ok(())
    }

    pub fn remove(&mut self, key: FFIArg) -> Result<(), SledError> {
        self.0.remove(encode_arg(key)?);
        Ok(())
    }

    pub fn get(&self, key: FFIArg) -> Result<Option<FFIValue>, SledError> {
        Ok(self
            .0
            .get(encode_arg(key)?)
            .and_then(|x| x.map(|x| decode_value(&x))))
    }
}

impl KeyType {
    pub fn from_str(key_type: Option<&str>) -> Result<Self, SledError> {
        match key_type {
            Some("integer") => Ok(KeyType::Integer),
            Some("string") => Ok(KeyType::String),
            Some("bytes") => Ok(KeyType::Bytes),
            None => Ok(KeyType::Any),
            Some(other) => Err(SledError::UnknownKeyType(other.to_string())),
        }
    }
}

impl SledDb {
    pub fn call_with_key<T, F: FnOnce(&Self, &[u8]) -> Result<T, SledError>>(
        &self,
        arg: FFIArg,
        thunk: F,
    ) -> Result<T, SledError> {
        match (arg, &self.key_type) {
            // TODO: Natively convert structs? Make FFI structs?
            (FFIArg::IntV(i), KeyType::Integer) => thunk(self, &i.to_be_bytes()),
            (FFIArg::StringV(s), KeyType::String) => thunk(self, s.as_bytes()),
            (FFIArg::StringRef(s), KeyType::String) => thunk(self, s.as_bytes()),
            (FFIArg::ByteVector(b), KeyType::Bytes) => thunk(self, b.as_slice()),

            (arg, k) => Err(SledError::Encoding(EncodeError::OtherString(format!(
                "Unable to encode key according to schema {:?} : {:?}",
                k, arg
            )))),
        }
    }

    pub fn open(path: String, key_type: Option<String>) -> Result<Self, SledError> {
        let db = Db::open_with_config(&sled::Config::new().path(path)).unwrap();

        let key_type = KeyType::from_str(key_type.as_ref().map(|x| x.as_str()))?;

        Ok(Self {
            db: DbOrTree::Db(db),
            key_type,
        })
    }

    pub fn open_tree(&self, name: String, key_type: Option<String>) -> Result<Self, SledError> {
        let key_type = KeyType::from_str(key_type.as_ref().map(|x| x.as_str()))?;

        match &self.db {
            DbOrTree::Db(db) => Ok(Self {
                db: DbOrTree::Tree(db.open_tree(name)?),
                key_type,
            }),
            DbOrTree::Tree(_) => Err(SledError::CannotOpenTreeFromTree),
        }
    }

    pub fn insert(&self, arg: FFIArg, value: FFIArg) -> Result<Option<FFIValue>, SledError> {
        self.call_with_key(arg, move |this: &Self, key| {
            this.db
                .insert(key, encode_arg(value)?)
                .map(|x| x.map(|x| decode_value(&x)))
                .map_err(SledError::Io)
        })
    }

    pub fn get(&self, key: FFIArg) -> Result<Option<FFIValue>, SledError> {
        self.call_with_key(key, move |this: &Self, key| {
            this.db
                .get(key)
                .map(|x| x.map(|x| decode_value(&x)))
                .map_err(SledError::Io)
        })
    }

    pub fn remove(&self, key: FFIArg) -> Result<Option<FFIValue>, SledError> {
        self.call_with_key(key, move |this: &Self, key| {
            this.db
                .remove(key)
                .map(|x| x.map(|x| decode_value(&x)))
                .map_err(SledError::Io)
        })
    }

    pub fn apply_batch(&self, mut batch: FFIArg) -> Result<(), SledError> {
        if let Some(value) = as_custom_type::<SledBatch>(&mut batch) {
            return Ok(self.db.apply_batch(std::mem::take(&mut value.0))?);
        }

        Err(SledError::TypeMismatch(format!(
            "Type mismatch - expected `SledBatch`, found: {:?}",
            batch
        )))
    }

    pub fn flush(&self) -> Result<(), SledError> {
        let _ = self.db.flush()?;
        Ok(())
    }

    // pub fn compare_and_swap(&self, key: Option<FFIArg>, old: Option<FFIArg>) -> Result {
    // }
}

pub fn as_custom_type<'a, T: Custom + 'static>(value: &'a mut FFIArg) -> Option<&'a mut T> {
    if let FFIArg::CustomRef(CustomRef { custom, .. }) = value {
        return as_underlying_ffi_type::<T>(custom.get_mut());
    }

    None
}

pub fn sled_module() -> FFIModule {
    let mut module = FFIModule::new("steel/sled");

    module
        .register_fn("db-open", SledDb::open)
        .register_fn("db-insert", SledDb::insert)
        .register_fn("db-get", SledDb::get)
        .register_fn("db-remove", SledDb::remove)
        .register_fn("db-open-tree", SledDb::open_tree)
        .register_fn("batch", SledBatch::new)
        .register_fn("batch-insert", SledBatch::insert)
        .register_fn("batch-get", SledBatch::get)
        .register_fn("batch-remove", SledBatch::remove)
        .register_fn("db-apply-batch", SledDb::apply_batch)
        .register_fn("db-flush", SledDb::flush);

    module
}
