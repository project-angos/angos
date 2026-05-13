use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestData {
    name: String,
    value: i32,
}

#[tokio::test]
async fn store_then_retrieve_value_roundtrips() {
    let cache = Cache::Stub(stub::Backend::new());
    let test_data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    cache.store("test_key", &test_data, 60).await.unwrap();
    let retrieved: TestData = cache.retrieve("test_key").await.unwrap().unwrap();

    assert_eq!(retrieved, test_data);
}

#[tokio::test]
async fn store_propagates_backend_error() {
    let backend = stub::Backend::new();
    backend.set_store_error(Some("Backend failure".to_string()));
    let cache = Cache::Stub(backend);
    let test_data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    let result = cache.store("test_key", &test_data, 60).await;

    assert!(matches!(result, Err(Error::Execution(_))));
}

#[derive(Debug, Serialize)]
struct UnserializableData {
    #[serde(serialize_with = "fail_serialization")]
    value: i32,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn fail_serialization<S>(_: &i32, _: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "Intentional serialization failure",
    ))
}

#[tokio::test]
async fn store_returns_err_on_serialization_failure() {
    let cache = Cache::Stub(stub::Backend::new());
    let bad_data = UnserializableData { value: 42 };

    let result = cache.store("test_key", &bad_data, 60).await;

    assert!(matches!(result, Err(Error::Execution(_))));
}

#[tokio::test]
async fn retrieve_returns_value_on_hit() {
    let backend = stub::Backend::new();
    let stored = TestData {
        name: "alice".to_string(),
        value: 7,
    };
    backend.set_data(Some(serde_json::to_string(&stored).unwrap()));
    let cache = Cache::Stub(backend);

    let result: Result<Option<TestData>, Error> = cache.retrieve("k").await;

    assert_eq!(result.unwrap(), Some(stored));
}

#[tokio::test]
async fn retrieve_returns_none_on_missing_key() {
    let cache = Cache::Stub(stub::Backend::new());

    let result: Result<Option<TestData>, Error> = cache.retrieve("k").await;

    assert_eq!(result.unwrap(), None);
}

#[tokio::test]
async fn retrieve_returns_err_on_backend_failure() {
    let backend = stub::Backend::new();
    backend.set_retrieve_error(Some("backend down".to_string()));
    let cache = Cache::Stub(backend);

    let result: Result<Option<TestData>, Error> = cache.retrieve("k").await;

    assert!(matches!(result, Err(Error::Execution(_))));
}

#[tokio::test]
async fn retrieve_returns_err_on_deserialization_failure() {
    let backend = stub::Backend::new();
    backend.set_data(Some("not valid json".to_string()));
    let cache = Cache::Stub(backend);

    let result: Result<Option<TestData>, Error> = cache.retrieve("k").await;

    assert!(matches!(result, Err(Error::Execution(_))));
}
