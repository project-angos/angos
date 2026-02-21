use std::{fs, path::PathBuf, str::FromStr};

use hyper::{HeaderMap, Method, http::request::Builder};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method},
};

use crate::{
    cache,
    command::server::{
        Error,
        auth::webhook::{
            Config, WebhookAuth, WebhookAuthorizer, build_header_name, build_header_value,
            build_headers, load_certificate_bundle, load_file, load_identity,
            set_forwarded_for_header, set_forwarded_headers, set_forwarded_host_header,
            set_forwarded_method_header, set_forwarded_proto_header, set_forwarded_uri_header,
            set_registry_action_header, set_registry_certificate_cn_header,
            set_registry_certificate_o_header, set_registry_digest_header,
            set_registry_identity_id_header, set_registry_namespace_header,
            set_registry_reference_header, set_registry_username_header,
        },
    },
    identity::{ClientIdentity, Route},
    oci::{Digest, Namespace, Reference},
    secret::Secret,
};

static TEST_BUNDLE: &str = r"-----BEGIN CERTIFICATE-----
MIIDgjCCAmqgAwIBAgIUFCYlDkKrxnJCnCtYXKvA9BaXnfowDQYJKoZIhvcNAQEL
BQAwWDELMAkGA1UEBhMCTFUxCzAJBgNVBAgMAkxVMRMwEQYDVQQHDApMdXhlbWJv
dXJnMRMwEQYDVQQKDApNeSBDb21wYW55MRIwEAYDVQQDDAlTZXJ2ZXIgQ0EwHhcN
MjUxMDA5MTcxNjIyWhcNMjYxMDA5MTcxNjIyWjBaMQswCQYDVQQGEwJMVTELMAkG
A1UECAwCTFUxEzARBgNVBAcMCkx1eGVtYm91cmcxEzARBgNVBAoMCk15IENvbXBh
bnkxFDASBgNVBAMMC2V4YW1wbGUuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAp4FMkW8y3+ZJDM1gZTSGpYk7WPHzv+eQOnWxcVif++j4EKxyIX3y
fKm11GokU7eKIbbUGcDEzBgh5V+VQoiweBC/S4mag86JCESX5dFv1jQ+KnjP6BkW
4bATqWwUwqUX/tXn3Oe/gTue64cU3nl7y6xOgX/jUF93GzVNS69Rz9E5DszeN1kw
zmh8dq88CZrReZ+nrQNFNmxFooqi/6bgnV8YlFfYT5ide+8LY+8Yho3ZcJ9cv530
TCCpX2xMfhqGFhfnVyR+Raj0/EU6PArIM+bXCw5a9llnU4ZQJBiaG6N0gSrPTHw6
kZyi9UE5KA4TwOtFcscFC/Rhm7pqY4z7mQIDAQABo0IwQDAdBgNVHQ4EFgQUgSvE
fVmU14s8Z4zAx3zv0x09pQMwHwYDVR0jBBgwFoAUCRVUTFXrNWkUWA8CwKljxF4R
FlgwDQYJKoZIhvcNAQELBQADggEBAFYCZiW1zpZAty9YFg/yNL2xw4XuDxJyvapT
4yd9LVhdIhNLSJo5dOsZynEFXOmvLpjEgfSRMAI0MhdqdqAjaDr2Wfg0P4VqfkC5
3BoRkwZ4sFDu9r7jiKvZplBO9qln+LxS20YFme1TpjzWzzCy1v/40xVF0PGONmiq
fTmTCQdUw11s7r6NwQPgrpJuyAX5iAY0MKccHMej5cnMy3HyjeCsByKdBqxOb+X4
IBcx+tr+Vvs6YWA7pd2UB6GbRbMgmELwVqkMFi6P7mzJv2PXsabzLzdSD41Xh/rL
pJ1J56iviNUViU6cY4Yy/Q9qe8aifhXXgaRgu5r8oBARAWo5LiE=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDkTCCAnmgAwIBAgIUL9X2kxKF7VYkVhPH/mNa3jlPp68wDQYJKoZIhvcNAQEL
BQAwWDELMAkGA1UEBhMCTFUxCzAJBgNVBAgMAkxVMRMwEQYDVQQHDApMdXhlbWJv
dXJnMRMwEQYDVQQKDApNeSBDb21wYW55MRIwEAYDVQQDDAlTZXJ2ZXIgQ0EwHhcN
MjUxMDA5MTcxNjIyWhcNMjYxMDA5MTcxNjIyWjBYMQswCQYDVQQGEwJMVTELMAkG
A1UECAwCTFUxEzARBgNVBAcMCkx1eGVtYm91cmcxEzARBgNVBAoMCk15IENvbXBh
bnkxEjAQBgNVBAMMCVNlcnZlciBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAMUKZYCdJF1ZsIGfjZeXfnjjWYnF7dunTBcJkdGgCi6D3Kpx3B+o61p/
0VFkbgOZWfRpCO/aXI/YSQ+t8SPMALZ2EITb1JWFlPzy6jkP1cYw+pXEWAkwHmLA
saEz8xZ629JlEEJ+7ZYKvkKffe1IiLS4Nswc8beW67+S1BrCRtiwlKWxKgMRQZs6
4Z5ERTacwB+nmaCNCYs18I8Qby28OHyyJsOSVviWDQflIUarypd3+gt7RvsjS9hl
/u05Se1lZnGTVlAIjbF0iItODSQBgWQ/GR/JsJiRazoZAHbeIsD+BysUcRPAClJl
3Za+v9FqA2OieFJLN1jypS01S7KUKGcCAwEAAaNTMFEwHQYDVR0OBBYEFAkVVExV
6zVpFFgPAsCpY8ReERZYMB8GA1UdIwQYMBaAFAkVVExV6zVpFFgPAsCpY8ReERZY
MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAG+IPbRcwR34T2ng
m65iFhXHa5G+Tsjtj2XvnbaL7gImbMub3CpVdcyk+jfKbEkQypAiC1M+FA36Nx9D
9EVrXAVieSj5sVewaPLlxyKmwMT/mUc8QghLtIU44uw4JU169Aq+csoXiVgjhwpr
BZ3/ZjKtbGFhVuo+bmzrX8fMcHDSgRZVMc74BCqtBUubKpDzopdxsu+DmQ3gQ/wJ
b+KEHQc1oSyOA2fh2K/CE0jSo8Rh5sAxMLbr+htmNS1AtQCoZbK6rM71fR1fKnV4
NlSR1ByFrL5KUbQYWIYPILTHyK6SSpwGqaETpuJHm0AUrBrBUZT3Qc4Ij/YDgDhS
iCKvlQA=
-----END CERTIFICATE-----
";

static TEST_CERT: &str = r"-----BEGIN CERTIFICATE-----
MIIDezCCAmOgAwIBAgIUEModFXgLFuzRPgiCn43Z7Xr+Au8wDQYJKoZIhvcNAQEL
BQAwWDELMAkGA1UEBhMCTFUxCzAJBgNVBAgMAkxVMRMwEQYDVQQHDApMdXhlbWJv
dXJnMRMwEQYDVQQKDApNeSBDb21wYW55MRIwEAYDVQQDDAlDbGllbnQgQ0EwHhcN
MjUxMDA5MTcxNjIyWhcNMjYxMDA5MTcxNjIyWjBTMQswCQYDVQQGEwJMVTELMAkG
A1UECAwCTFUxEzARBgNVBAcMCkx1eGVtYm91cmcxDzANBgNVBAoMBmFkbWluczER
MA8GA1UEAwwIcGhpbGlwcGUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDGrO6cHAkKrTEEylFLQnweym1fzbdLCN3IV/YwtwOfiFZjx3yxcfaQXyyKaWQg
lyVCuih/QJRFxEnZVmczmbNplEwbN/Ky4siSTZbF2Tt/9vqg+mlFZfYWO1F9tyuZ
5O+IuaEO2thecqFMEHAIh2k5iSYo/Jx5RD3EUQ99FCaCjWWrY85laWzpb4S9NZRc
pFo0/I1OF2PSJaFaHvQal7OMIHoXyF5AVlL6Dk9gSJ/poTVBjzn6HiI9JaoFr8AQ
TeQwWUik0NrVOvzBhIYhCnLY/UyOhI+FkYgQ2ivSgWnYvc2FtODaQ+WcvYV5lMpO
yrzUGtGUvP+W8DFuG/u/56MPAgMBAAGjQjBAMB0GA1UdDgQWBBRndHyZYvWkSA+a
N2MnoNDMMckCPDAfBgNVHSMEGDAWgBQlAxQ0Idm1PwjvOpOACBgOL8wwlTANBgkq
hkiG9w0BAQsFAAOCAQEAWWIPGg3PEs21XhuL5SIANOhyXQkwTzqHUsi7sPWKFSWv
kIYgDKmj2faPEJZ6PG4lsfFEI+7Gr+/P+gEbvvwHjmekR434hPHxeAvwQqacYtMj
2CkrkvpQkNdKZFcFkPaG6t48qJWOcVV4esuXQ/irlhYQBCqrQ6zsFDQ42pEtTdJ7
LmCKvaKYTMYeiGt0XLEkz+3MS6AW2RSKqKsV53PEKYx/zxusVg1GuspYCzG5o8xm
ytDuL5zW+HB/R/unvX0QwwunrXe1KE2xFiYPzcXOYIA8eoKDBpeyl7u4J5Fd7Vkq
C6stttHEnme/iUDVYcjLLE9nG+CT/MZRg7O1j5JDVA==
-----END CERTIFICATE-----
";

static TEST_KEY: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGrO6cHAkKrTEE
ylFLQnweym1fzbdLCN3IV/YwtwOfiFZjx3yxcfaQXyyKaWQglyVCuih/QJRFxEnZ
VmczmbNplEwbN/Ky4siSTZbF2Tt/9vqg+mlFZfYWO1F9tyuZ5O+IuaEO2thecqFM
EHAIh2k5iSYo/Jx5RD3EUQ99FCaCjWWrY85laWzpb4S9NZRcpFo0/I1OF2PSJaFa
HvQal7OMIHoXyF5AVlL6Dk9gSJ/poTVBjzn6HiI9JaoFr8AQTeQwWUik0NrVOvzB
hIYhCnLY/UyOhI+FkYgQ2ivSgWnYvc2FtODaQ+WcvYV5lMpOyrzUGtGUvP+W8DFu
G/u/56MPAgMBAAECggEAKWUoxkAVJjNVzlC1RYARynyU82wyb6DmTPL+6cGIMLpA
fcO32GUNYaFi72fsI9o6Oj/9Zh43hp3SYUVedWLl/e6XOOicWedksQ8XhhuwCQaV
y+rA+mO3NYSggxgiLouD2TIMO8MfZ/ZsYyPdo/lK1GEeIVYY6C9uyzO0jXQgXfzo
FUe9+U5KBMgWKSigv6oGFEQjTa30r5LsPMo4BvZu9dS80KRSSRFE9BPhg/u8aM/O
gXTanZzz/v/dxwrE6Qq8pGIYiCjekcwPU/XKey/5tbaaAM61nZGdnwmMLXRO3/3f
ktDoVnnB5QfTGhbh9IcrxJ5oj6NpFUUnGvrmp3kU2QKBgQDpE1wONEqEhBe876Y+
qKxzDVPiAArhl/t0GGTunPcxCQshWaiLrFRyInvuoFp1PE9/PeFww2fMbywS0TLz
b7j/fBRGCHCAQ4hH2Ine3mhYHvi8gXLX1C9XOCmOUGNxn+9zYs7Y3eaLPN3LKGmw
VJd4IurtmpEfLNQ99RHZkMxQ0wKBgQDaN2howGxeBIcZzQUqdx4Yyx24HOzMqhiO
tnXIgEMENClDa16NxEqBrBSIPCXgppp70QjXnzhW0V7x6H4maeHlahIvzomo+nLP
6AocqKsfvPSTgv9pb0sldE+9537e0Ck6+8NYVNCIMqjjZRjz7jfWVAuN2XA8al4C
zlKjRhPfVQKBgBzqxPoSLMiiJtvPE94kSTkBB0472R3CIHV37VXZbaXMzG+30vx5
RgTfGGczx+VRtT9BKy41YDRx+pLfF6YyT06LU2yY8XRIbKkVSY24JFQCi7O/j8MN
VU5J7oX0nVHkmO3E7YrkhQzzYUUqX2p8JErIckNGcQjgI/kH5c4Lc/33AoGBAL9K
8Tla7eShXXmts4idcYHUlRHwMVndBrgclTYV0inePAoBFpt6ZsI0Aq/G4oGEK0q9
XV4AEthwpCW2ZNfx2/hLuvOzwBOksX82b57d8V1aPKEPpi1cRejohHr6c8qJeotd
ZsqJV2D93/Wvi2dS/hniBVrtMSmVKSKWkfTVmtgdAoGBANpxg6Vw1q0KX/9Xw/b4
Dibexd+opAZR8v9/sWQbShtgb6HN1HrnjpCCRoCML5OgAfT5jZatYhXG958aJVoX
QZNkCzFWJ+PY4vUsqDTqpcwtd+VWlwepuaWp7O96i2vhHvLJ9z6/gYHg6RxQ96nU
4x20RWq1FM8sACYdrLbayZCL
-----END PRIVATE KEY-----
";

#[test]
fn test_config_deserialize() {
    let valid_config = r#"
        url = "https://example.com"
        timeout_ms = 1000
        basic_auth = { username = "user", password = "pass" }
    "#;

    let config: Config = toml::from_str(valid_config).unwrap();

    assert!(config.validate().is_ok());
    assert_eq!(config.url, "https://example.com");
    assert_eq!(config.timeout_ms, 1000);
    assert!(
        matches!(config.auth, Some(WebhookAuth::BasicAuth { username, password }) if username == "user" && password.expose() == "pass")
    );
    assert!(config.client_certificate_bundle.is_none());
    assert!(config.client_private_key.is_none());
    assert!(config.server_ca_bundle.is_none());
    assert!(config.forward_headers.is_empty());
    assert_eq!(config.cache_ttl, 60);

    let valid_config = r#"
        url = "https://example.com"
        timeout_ms = 1000
        bearer_token = "hello-token"
    "#;

    let config: Config = toml::from_str(valid_config).unwrap();

    assert!(config.validate().is_ok());
    assert_eq!(config.url, "https://example.com");
    assert_eq!(config.timeout_ms, 1000);
    assert!(
        matches!(config.auth, Some(WebhookAuth::BearerToken(token)) if token.expose() == "hello-token")
    );
    assert!(config.client_certificate_bundle.is_none());
    assert!(config.client_private_key.is_none());
    assert!(config.server_ca_bundle.is_none());
    assert!(config.forward_headers.is_empty());
    assert_eq!(config.cache_ttl, 60);
}

#[test]
fn test_config_validate() {
    let valid_config = Config {
        url: "https://example.com".to_string(),
        timeout_ms: 1000,
        auth: Some(WebhookAuth::BearerToken(Secret::new("token".to_string()))),
        client_certificate_bundle: Some("/valid/path/to/cert.pem".into()),
        client_private_key: Some("/valid/path/to/key.pem".into()),
        server_ca_bundle: Some("/valid/path/to/ca.pem".into()),
        forward_headers: vec!["X-Custom-Header".to_string()],
        cache_ttl: 60,
    };
    assert!(valid_config.validate().is_ok());

    let mut invalid_config = valid_config.clone();
    invalid_config.client_private_key = None;
    assert!(invalid_config.validate().is_err());

    let mut invalid_config = valid_config.clone();
    invalid_config.client_certificate_bundle = None;
    assert!(invalid_config.validate().is_err());

    let mut invalid_config = valid_config.clone();
    invalid_config.url = "@invalid-url@".to_string();
    assert!(invalid_config.validate().is_err());
}

#[test]
fn test_load_file() {
    let content = "test content";

    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("test.txt");
    fs::write(&file_path, content).unwrap();

    let loaded_content = load_file(&file_path).unwrap();
    assert_eq!(loaded_content, content.as_bytes());

    let invalid_path = load_file(&PathBuf::from("/invalid/path/to/file"));
    assert!(matches!(invalid_path, Err(Error::Initialization(_))));
}

#[test]
fn test_load_certificate_bundle() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("bundle.pem");
    fs::write(&file_path, TEST_BUNDLE).unwrap();

    let loaded_certificates = load_certificate_bundle(&file_path).unwrap();
    assert_eq!(loaded_certificates.len(), 2);
}

#[test]
fn test_load_certificate_invalid() {
    let content = "-----BEGIN INVALID CERTIFICATE-----LOLNOP-----END CERTIFICATE-----";
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("test.txt");
    fs::write(&file_path, content).unwrap();

    let invalid_certificates = load_certificate_bundle(&file_path);
    assert!(matches!(
        invalid_certificates,
        Err(Error::Initialization(_))
    ));
}

#[test]
fn test_load_identity() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cert_file_path = tmp_dir.path().join("certificate.pem");
    fs::write(&cert_file_path, TEST_CERT).unwrap();

    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, TEST_KEY).unwrap();

    let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
    assert!(matches!(identity, Ok(Some(_))));

    let cert_file_path = tmp_dir.path().join("certificate.pem");
    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, TEST_BUNDLE).unwrap();

    let identity = load_identity(Some(&cert_file_path), Some(&key_file_path));
    assert!(matches!(identity, Err(Error::Initialization(_))));

    let identity = load_identity(None, None);
    assert!(matches!(identity, Ok(None)));
}

#[test]
fn test_build_header_name() {
    let header = "X-Custom-Header";
    let header = build_header_name(header);
    assert!(header.is_ok());

    let header = "Invalid Header!";
    let header = build_header_name(header);
    assert!(matches!(header, Err(Error::Execution(_))));
}
#[test]
fn test_build_header_value() {
    let value = "Some value";
    let value = build_header_value(value);
    assert!(value.is_ok());

    let value = "Invalid\r\nValue";
    let value = build_header_value(value);
    assert!(matches!(value, Err(Error::Execution(_))));
}

#[test]
fn test_set_forwarded_method_header() {
    let request = Builder::new()
        .method(Method::POST)
        .uri("https://example.com/path")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_method_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Method").unwrap(), "POST");
}

#[test]
fn test_set_forwarded_proto_header() {
    let request = Builder::new()
        .uri("https://example.com/path")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_proto_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");

    let request = Builder::new()
        .uri("http://example.com/path")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_proto_header(&parts, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "http");
}

#[test]
fn test_set_forwarded_host_header() {
    let request = Builder::new()
        .uri("https://example.com/path")
        .header("Host", "example.com")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    set_forwarded_host_header(&parts, &mut headers);
    assert_eq!(headers.get("X-Forwarded-Host").unwrap(), "example.com");
}

#[test]
fn test_set_forwarded_uri_header() {
    let request = Builder::new()
        .uri("https://example.com/v2/test/manifests/latest")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_uri_header(&parts, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Forwarded-Uri").unwrap(),
        "https://example.com/v2/test/manifests/latest"
    );
}

#[test]
fn test_set_forwarded_for_header() {
    let mut identity = ClientIdentity::new(None);
    identity.client_ip = Some("192.168.1.1".to_string());

    let mut headers = HeaderMap::new();

    assert!(set_forwarded_for_header(&identity, &mut headers).is_ok());
    assert_eq!(headers.get("X-Forwarded-For").unwrap(), "192.168.1.1");

    let identity_no_ip = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_forwarded_for_header(&identity_no_ip, &mut headers).is_ok());
    assert!(headers.get("X-Forwarded-For").is_none());
}

#[test]
fn test_set_registry_action_header() {
    let route = Route::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_action_header(&route, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Action").unwrap(), "get-api-version");
}

#[test]
fn test_set_registry_namespace_header() {
    let route = Route::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag("latest".to_string()),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_namespace_header(&route, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Registry-Namespace").unwrap(),
        "test-namespace"
    );

    let route = Route::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_namespace_header(&route, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Namespace").is_none());
}

#[test]
fn test_set_registry_reference_header() {
    let route = Route::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag("v1.0.0".to_string()),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_reference_header(&route, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Reference").unwrap(), "v1.0.0");

    let route = Route::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_reference_header(&route, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Reference").is_none());
}

#[test]
fn test_set_registry_digest_header() {
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let digest = Digest::from_str(digest).unwrap();
    let route = Route::DeleteBlob {
        namespace: Namespace::new("test-namespace").unwrap(),
        digest: digest.clone(),
    };
    let mut headers = HeaderMap::new();

    assert!(set_registry_digest_header(&route, &mut headers).is_ok());
    assert_eq!(
        headers.get("X-Registry-Digest").unwrap(),
        &digest.to_string()
    );

    let route = Route::ApiVersion;
    let mut headers = HeaderMap::new();

    assert!(set_registry_digest_header(&route, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Digest").is_none());
}

#[test]
fn test_set_registry_username_header() {
    let mut identity = ClientIdentity::new(None);
    identity.username = Some("testuser".to_string());
    let mut headers = HeaderMap::new();

    assert!(set_registry_username_header(&identity, &mut headers).is_ok());

    assert_eq!(headers.get("X-Registry-Username").unwrap(), "testuser");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_username_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Username").is_none());
}

#[test]
fn test_set_registry_identity_id_header() {
    let user_id = "user-id-123".to_string();
    let mut identity = ClientIdentity::new(None);
    identity.id = Some(user_id.clone());

    let mut headers = HeaderMap::new();

    assert!(set_registry_identity_id_header(&identity, &mut headers).is_ok());
    assert_eq!(headers.get("X-Registry-Identity-ID").unwrap(), &user_id);

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();
    assert!(set_registry_username_header(&identity, &mut headers).is_ok());

    assert!(headers.get("X-Registry-Identity-ID").is_none());
}

#[test]
fn test_set_registry_certificate_cn_header() {
    let mut identity = ClientIdentity::new(None);
    identity.certificate.common_names = vec!["cn1".to_string(), "cn2".to_string()];
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_cn_header(&identity, &mut headers).is_ok());

    let values: Vec<_> = headers
        .get_all("X-Registry-Certificate-CN")
        .iter()
        .collect();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "cn1");
    assert_eq!(values[1], "cn2");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_cn_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Certificate-CN").is_none());
}

#[test]
fn test_set_registry_certificate_o_header() {
    let mut identity = ClientIdentity::new(None);
    identity.certificate.organizations = vec!["org1".to_string(), "org2".to_string()];

    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_o_header(&identity, &mut headers).is_ok());

    let values: Vec<_> = headers.get_all("X-Registry-Certificate-O").iter().collect();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "org1");
    assert_eq!(values[1], "org2");

    let identity = ClientIdentity::new(None);
    let mut headers = HeaderMap::new();

    assert!(set_registry_certificate_o_header(&identity, &mut headers).is_ok());
    assert!(headers.get("X-Registry-Certificate-O").is_none());
}

#[test]
fn test_set_forwarded_headers() {
    let request = Builder::new()
        .uri("https://example.com/path")
        .header("X-Custom-Header", "custom-value")
        .header("X-Another-Header", "another-value")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();
    let mut headers = HeaderMap::new();

    let forward_headers = vec![
        "X-Custom-Header".to_string(),
        "X-Another-Header".to_string(),
    ];
    assert!(set_forwarded_headers(&forward_headers, &parts, &mut headers).is_ok());

    assert_eq!(headers.get("X-Custom-Header").unwrap(), "custom-value");
    assert_eq!(headers.get("X-Another-Header").unwrap(), "another-value");
}

#[test]
fn test_build_headers() {
    let request = Builder::new()
        .method(Method::GET)
        .uri("https://example.com/v2/test-namespace/manifests/latest")
        .header("Host", "example.com")
        .header("X-Custom-Header", "custom-value")
        .body(())
        .unwrap();

    let (parts, ()) = request.into_parts();

    let route = Route::GetManifest {
        namespace: Namespace::new("test-namespace").unwrap(),
        reference: Reference::Tag("latest".to_string()),
    };

    let mut identity = ClientIdentity::new(None);
    identity.username = Some("testuser".to_string());
    identity.client_ip = Some("192.168.1.1".to_string());

    let forward_headers = vec!["X-Custom-Header".to_string()];

    let headers = build_headers(&forward_headers, &route, &identity, &parts);

    assert!(headers.is_ok());
    let headers = headers.unwrap();

    assert_eq!(headers.get("X-Forwarded-Method").unwrap(), "GET");
    assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");
    assert_eq!(headers.get("X-Forwarded-Host").unwrap(), "example.com");
    assert!(headers.get("X-Forwarded-Uri").is_some());
    assert_eq!(headers.get("X-Forwarded-For").unwrap(), "192.168.1.1");
    assert_eq!(headers.get("X-Registry-Action").unwrap(), "get-manifest");
    assert_eq!(
        headers.get("X-Registry-Namespace").unwrap(),
        "test-namespace"
    );
    assert_eq!(headers.get("X-Registry-Reference").unwrap(), "latest");
    assert_eq!(headers.get("X-Registry-Username").unwrap(), "testuser");
    assert_eq!(headers.get("X-Custom-Header").unwrap(), "custom-value");
}

fn build_test_config(
    url: String,
    server_ca_bundle: Option<PathBuf>,
    client_certificate_bundle: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
) -> Config {
    Config {
        url,
        timeout_ms: 1000,
        auth: Some(WebhookAuth::BearerToken(Secret::new("token".to_string()))),
        client_certificate_bundle,
        client_private_key,
        server_ca_bundle,
        forward_headers: vec!["X-Custom-Header".to_string()],
        cache_ttl: 60,
    }
}

#[test]
fn test_new_invalid_mtls() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cert_file_path = tmp_dir.path().join("certificate.pem");
    fs::write(&cert_file_path, TEST_CERT).unwrap();

    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, TEST_BUNDLE).unwrap();

    let ca_file_path = tmp_dir.path().join("ca.pem");
    fs::write(&ca_file_path, TEST_BUNDLE).unwrap();

    let config = build_test_config(
        "https://example.com".to_string(),
        Some(ca_file_path),
        Some(cert_file_path),
        Some(key_file_path),
    );
    let webhook = WebhookAuthorizer::new(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

    assert!(matches!(webhook, Err(Error::Initialization(_))));
}

#[test]
fn test_new_mtls() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let cert_file_path = tmp_dir.path().join("certificate.pem");
    fs::write(&cert_file_path, TEST_CERT).unwrap();

    let key_file_path = tmp_dir.path().join("private-key.pem");
    fs::write(&key_file_path, TEST_KEY).unwrap();

    let ca_file_path = tmp_dir.path().join("ca.pem");
    fs::write(&ca_file_path, TEST_BUNDLE).unwrap();

    let config = build_test_config(
        "https://example.com".to_string(),
        Some(ca_file_path),
        Some(cert_file_path),
        Some(key_file_path),
    );
    let webhook = WebhookAuthorizer::new(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

    assert!(webhook.is_ok());
}

#[test]
fn test_new_simple() {
    let config = build_test_config("https://example.com".to_string(), None, None, None);
    let webhook = WebhookAuthorizer::new(
        "test".to_string(),
        config,
        cache::Config::Memory.to_backend().unwrap(),
    );

    assert!(webhook.is_ok());
}

#[tokio::test]
async fn test_authorize_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(mock_server.uri(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
}

#[tokio::test]
async fn test_authorize_denied() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(mock_server.uri(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(
        webhook.authorize(&route, &identity, &parts).await,
        Ok(false)
    );
}

#[tokio::test]
async fn test_authorize_with_bearer_token() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(mock_server.uri(), None, None, None);
    config.auth = Some(WebhookAuth::BearerToken(Secret::new(
        "test-token".to_string(),
    )));

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
}

#[tokio::test]
async fn test_authorize_with_basic_auth() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("Authorization", "Basic dGVzdHVzZXI6dGVzdHBhc3M="))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(mock_server.uri(), None, None, None);
    config.auth = Some(WebhookAuth::BasicAuth {
        username: "testuser".to_string(),
        password: Secret::new("testpass".to_string()),
    });

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
}

#[tokio::test]
async fn test_authorize_sends_correct_headers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(header("X-Forwarded-Method", "GET"))
        .and(header("X-Registry-Action", "get-api-version"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = build_test_config(mock_server.uri(), None, None, None);
    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .method(Method::GET)
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
}

#[tokio::test]
async fn test_authorize_uses_cache() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut config = build_test_config(mock_server.uri(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
    assert_eq!(webhook.authorize(&route, &identity, &parts).await, Ok(true));
}

#[tokio::test]
async fn test_authorize_network_error_denies() {
    let mut config = build_test_config("http://localhost:1".to_string(), None, None, None);
    config.auth = None;

    let cache = cache::Config::Memory.to_backend().unwrap();
    let webhook = WebhookAuthorizer::new("test".to_string(), config, cache).unwrap();

    let route = Route::ApiVersion;
    let identity = ClientIdentity::new(None);

    let request = Builder::new()
        .uri("https://example.com/v2/")
        .body(())
        .unwrap();
    let (parts, ()) = request.into_parts();

    assert_eq!(
        webhook.authorize(&route, &identity, &parts).await,
        Ok(false)
    );
}
