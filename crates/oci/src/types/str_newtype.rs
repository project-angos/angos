/// The conversions, views and serde impls of a `struct $t(String)` whose
/// validating `new(&str)` every constructor funnels through.
macro_rules! str_newtype {
    ($t:ident) => {
        impl ::std::str::FromStr for $t {
            type Err = $crate::types::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $t {
            type Error = $crate::types::Error;

            fn try_from(s: String) -> Result<Self, Self::Error> {
                Self::new(&s)
            }
        }

        impl TryFrom<&str> for $t {
            type Error = $crate::types::Error;

            fn try_from(s: &str) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl ::std::fmt::Display for $t {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $t {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::ops::Deref for $t {
            type Target = str;

            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::borrow::Borrow<str> for $t {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq<str> for $t {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }

        impl PartialEq<&str> for $t {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }

        impl ::serde::Serialize for $t {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $t {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                let s: String = ::serde::Deserialize::deserialize(deserializer)?;
                Self::new(&s).map_err(::serde::de::Error::custom)
            }
        }
    };
}
