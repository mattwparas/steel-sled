use std::sync::Mutex;

use abi_stable::std_types::{RHashMap, RString, RVec, Tuple2};
use bincode::{
    Decode, Encode,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use sled::Db;
use steel::{
    rvals::Custom,
    steel_vm::ffi::{FFIArg, FFIModule, FFIValue, RegisterFFIFn},
};

steel::declare_module!(build_module);

pub fn build_module() -> FFIModule {
    sled_module()
}

enum KeyType {
    Integer,
    String,
}

// Values have to be encoded correctly, values are encoded
// more or less directly as the enum values unless otherwise specified.
struct SledDb {
    // I'd love to not have to mutex this since we're already guarded behind a mutex.
    // I can probably relax the constraints on this since this whole thing will be guarded.
    db: Mutex<Db<1024>>,
    key_type: KeyType,
}

impl Custom for SledDb {}

struct BincodeWrapper {
    value: FFIValue,
}

pub(crate) fn encode_slice_len<E: Encoder>(encoder: &mut E, len: usize) -> Result<(), EncodeError> {
    (len as u64).encode(encoder)
}

impl Encode for BincodeWrapper {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Manually do our recursion here
        fn encode_inner<EI: bincode::enc::Encoder>(
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
                        encode_inner(value, encoder)?;
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
                        encode_inner(key, encoder)?;
                        encode_inner(value, encoder)?;
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

        encode_inner(&self.value, encoder)
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
                    // Push on the size, alongside a vector with the capacity
                    // that we want.
                    let size = u64::decode(decoder)?;

                    // Now, we'll start pushing values on.
                    // Once those values are done, we'll pop it off, and otherwise
                    // push it in to the remaining stack
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

pub fn encode_value(value: FFIValue) -> Vec<u8> {
    let val = BincodeWrapper { value };
    bincode::encode_to_vec(val, bincode::config::standard()).unwrap()
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
    let decoded = decode_value(encoded.as_slice());

    dbg!(decoded);
}

// Conversions for the purposes of storing the values
// in sled
fn arg_to_value(value: FFIArg) -> Option<FFIValue> {
    match value {
        FFIArg::StringRef(rstr) => Some(FFIValue::StringV(rstr.into())),
        FFIArg::BoolV(b) => Some(FFIValue::BoolV(b)),
        FFIArg::NumV(n) => Some(FFIValue::NumV(n)),
        FFIArg::IntV(i) => Some(FFIValue::IntV(i)),
        FFIArg::Void => Some(FFIValue::Void),
        FFIArg::StringV(rstring) => Some(FFIValue::StringV(rstring)),
        FFIArg::Vector(rvec) => Some(FFIValue::Vector(
            rvec.into_iter().filter_map(arg_to_value).collect(),
        )),
        FFIArg::CharV { c } => Some(FFIValue::CharV { c }),
        // FFIArg::HashMap(rhash_map) => todo!(),
        FFIArg::ByteVector(rvec) => Some(FFIValue::ByteVector(rvec)),
        _ => None,
    }
}

impl SledDb {
    // Set up key type properly
    pub fn open(path: String, key_type: String) -> Self {
        let db = Db::open_with_config(&sled::Config::new().path(path)).unwrap();

        let key_type = match key_type.as_str() {
            "integer" => KeyType::Integer,
            "string" => KeyType::String,
            _ => panic!("Unexpected key type"),
        };

        Self {
            db: Mutex::new(db),
            key_type,
        }
    }

    // Get via key? And then do some stuff
    // after that? Serializing? Deserializing?
    pub fn insert(&self, arg: FFIArg, value: FFIArg) {
        match (arg, &self.key_type) {
            (FFIArg::IntV(i), KeyType::Integer) => {
                let value = arg_to_value(value).unwrap();
                self.db
                    .lock()
                    .unwrap()
                    .insert(i.to_be_bytes(), encode_value(value))
                    .unwrap();
            }
            // Set up using the proper thing
            (FFIArg::StringV(s), KeyType::String) => {
                let value = arg_to_value(value).unwrap();
                self.db
                    .lock()
                    .unwrap()
                    .insert(s.as_bytes(), encode_value(value))
                    .unwrap();
            }

            (FFIArg::StringRef(s), KeyType::String) => {
                let value = arg_to_value(value).unwrap();
                self.db
                    .lock()
                    .unwrap()
                    .insert(s.as_bytes(), encode_value(value))
                    .unwrap();
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn get(&self, key: FFIArg) -> Option<FFIValue> {
        let value = match (key, &self.key_type) {
            (FFIArg::IntV(i), KeyType::Integer) => {
                self.db.lock().unwrap().get(i.to_be_bytes()).unwrap()
            }
            // Set up using the proper thing
            (FFIArg::StringV(s), KeyType::String) => {
                self.db.lock().unwrap().get(s.as_bytes()).unwrap()
            }
            (FFIArg::StringRef(s), KeyType::String) => {
                self.db.lock().unwrap().get(s.as_bytes()).unwrap()
            }
            _ => {
                todo!()
            }
        };

        value.map(|x| decode_value(&x))
    }

    pub fn remove(&self, key: FFIArg) -> Option<FFIValue> {
        let value = match (key, &self.key_type) {
            (FFIArg::IntV(i), KeyType::Integer) => {
                self.db.lock().unwrap().remove(i.to_be_bytes()).unwrap()
            }
            // Set up using the proper thing
            (FFIArg::StringV(s), KeyType::String) => {
                self.db.lock().unwrap().remove(s.as_bytes()).unwrap()
            }
            (FFIArg::StringRef(s), KeyType::String) => {
                self.db.lock().unwrap().remove(s.as_bytes()).unwrap()
            }
            _ => {
                todo!()
            }
        };

        value.map(|x| decode_value(&x))
    }
}

pub fn sled_module() -> FFIModule {
    let mut module = FFIModule::new("steel/sled");

    module
        .register_fn("db-open", SledDb::open)
        .register_fn("db-insert", SledDb::insert)
        .register_fn("db-get", SledDb::get)
        .register_fn("db-remove", SledDb::remove);

    module
}
