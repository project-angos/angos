---
displayed_sidebar: howto
sidebar_position: 12
title: "Troubleshooting"
---

# Troubleshoot Common Issues

Solutions for frequently encountered problems with Angos.

## Debug Logging

Enable detailed logging to diagnose issues:

```bash
# General debug
RUST_LOG=debug ./angos server

# Specific modules
RUST_LOG=info,angos::command::server::auth=debug ./angos server

# Multiple modules
RUST_LOG=info,angos::configuration=debug,angos::cache=debug ./angos server
```

Useful modules:
- `angos::configuration` - Config loading/watching
- `angos::command::server::auth` - Authentication
- `angos::cache` - Pull-through cache
- `angos::registry::access_policy` - Policy evaluation

---

## Authentication Issues

### "unauthorized: Access denied"

**Cause**: No credentials provided or invalid credentials.

**Solutions**:
1. Check credentials are correct:
   ```bash
   curl -u user:password http://localhost:5000/v2/
   ```

2. For OIDC, verify token is valid:
   ```bash
   curl -H "Authorization: Bearer $TOKEN" http://localhost:5000/v2/
   ```

3. Check access policy allows the action:
   ```toml
   [global.access_policy]
   default_allow = false
   rules = ["identity.username != ''"]
   ```

### "forbidden: access denied"

**Cause**: Authenticated but policy denies access.

**Solutions**:
1. Enable policy debug logging:
   ```bash
   RUST_LOG=angos::registry::access_policy=debug
   ```

2. Check rules match your identity and action
3. For OIDC, always check null first:
   ```toml
   rules = ["identity.oidc != null && identity.oidc.claims['repo'].startsWith('myorg/')"]
   ```

### OIDC Token Rejected

**Causes**:
- Token expired
- Issuer mismatch
- Audience mismatch
- JWKS fetch failure

**Solutions**:
1. Verify token hasn't expired
2. Check issuer exactly matches configuration
3. Verify audience if `required_audience` is set
4. Ensure registry can reach OIDC provider:
   ```bash
   curl https://token.actions.githubusercontent.com/.well-known/jwks
   ```

### mTLS Certificate Rejected

**Causes**:
- Certificate not signed by trusted CA
- Certificate expired
- Wrong certificate format

**Solutions**:
1. Verify certificate chain:
   ```bash
   openssl verify -CAfile ca.pem client.pem
   ```

2. Check expiration:
   ```bash
   openssl x509 -in client.pem -noout -dates
   ```

3. Ensure PEM format for all certificates

---

## Push/Pull Issues

### "manifest unknown"

**Cause**: Manifest doesn't exist.

**Solutions**:
1. Verify the tag/digest exists:
   ```bash
   curl http://localhost:5000/v2/namespace/image/tags/list
   ```

2. For pull-through cache, check upstream connectivity
3. Check namespace spelling

### "blob unknown"

**Cause**: Blob not found in storage.

**Solutions**:
1. Re-push the image
2. Check storage backend is accessible
3. For S3, verify bucket permissions

### "Tag immutable"

**Cause**: Attempting to overwrite an immutable tag.

**Solutions**:
1. Use a different tag
2. Add tag to exclusions:
   ```toml
   immutable_tags_exclusions = ["^latest$", "^your-tag$"]
   ```
3. Disable immutability for the repository

### Push Timeout

**Causes**:
- Large blob
- Slow network
- S3 timeout

**Solutions**:
1. Increase timeouts in S3 config:
   ```toml
   [blob_store.s3]
   operation_timeout_secs = 1800
   operation_attempt_timeout_secs = 600
   ```

2. Check network connectivity
3. Consider chunked uploads

---

## Pull-Through Cache Issues

### "unexpected status code 401"

**Cause**: Upstream credentials invalid.

**Solutions**:
1. Verify upstream credentials:
   ```bash
   docker login registry-1.docker.io
   ```

2. Check credentials in config:
   ```toml
   [[repository."library".upstream]]
   url = "https://registry-1.docker.io"
   username = "correct-user"
   password = "correct-pass"
   ```

### Cache Not Working

**Symptoms**: Every pull contacts upstream.

**Solutions**:
1. Enable cache debug logging:
   ```bash
   RUST_LOG=angos::cache=debug
   ```

2. Check immutable tags for optimization:
   ```toml
   [repository."library"]
   immutable_tags = true
   ```

3. Verify storage is writable

### Rate Limited by Upstream

**Symptoms**: 429 errors or slow pulls.

**Solutions**:
1. Add upstream credentials (higher limits)
2. Enable immutable tags to reduce checks
3. Add more upstreams for fallback

---

## Storage Issues

### Filesystem Permissions

**Symptoms**: Permission denied errors.

**Solutions**:
```bash
# Check ownership
ls -la /data/registry

# Fix permissions
sudo chown -R $(id -u):$(id -g) /data/registry
```

### S3 Connection Errors

**Symptoms**: Timeout or connection refused.

**Solutions**:
1. Verify endpoint URL:
   ```bash
   curl $S3_ENDPOINT
   ```

2. Check credentials:
   ```bash
   aws s3 ls s3://your-bucket --endpoint-url $S3_ENDPOINT
   ```

3. Verify region is correct

### "lock already held"

**Cause**: Concurrent operations on same resource.

**Solutions for Redis locking**:
1. Configure Redis locking (use `lock_strategy.redis` form):
   ```toml
   [metadata_store.fs.lock_strategy.redis]
   url = "redis://localhost:6379"
   ttl = 10
   max_retries = 100
   ```

2. Increase retry settings
3. Check for stuck processes

**Solutions for S3 locking**:
1. Stale locks under the `_locks/` prefix are automatically recovered after TTL expiry
2. If operations take longer than the TTL, increase `ttl_secs` (minimum: 9):
   ```toml
   [metadata_store.s3.lock_strategy.s3]
   ttl_secs = 60
   ```
   The heartbeat interval is automatically set to `ttl_secs / 3`, so this example will heartbeat every 20 seconds.

3. For high contention, increase `max_retries` and `retry_delay_ms`:
   ```toml
   [metadata_store.s3.lock_strategy.s3]
   max_retries = 200
   retry_delay_ms = 100
   ```

4. If the startup probe fails, your S3 provider may not support conditional writes — fall back to Redis locking instead. S3 locking is only supported when using S3 for metadata storage; it cannot be used with filesystem metadata stores.

---

## Configuration Issues

### Config Not Reloading

**Symptoms**: Changes not taking effect.

**Solutions**:
1. Check config is valid:
   ```bash
   ./angos -c config.toml server  # Will error on invalid
   ```

2. Some settings require restart:
   - `bind_address`, `port`
   - TLS enable/disable
   - Storage backend type

### TLS Certificate Errors

**Solutions**:
1. Verify certificate files:
   ```bash
   openssl x509 -in server.crt -noout -text
   openssl rsa -in server.key -check
   ```

2. Check certificate matches key:
   ```bash
   openssl x509 -noout -modulus -in server.crt | openssl md5
   openssl rsa -noout -modulus -in server.key | openssl md5
   ```

3. Ensure full chain is included

---

## Web UI Issues

### Blank Page

**Solutions**:
1. Check browser console for errors
2. Clear browser cache
3. Verify `ui.enabled = true`
4. Check access policy allows `ui-asset`

### 403 on Browse

**Solutions**:
Add to access policy:
```toml
rules = [
  "request.action == 'ui-asset' || request.action == 'ui-config'",
  "identity.username != '' && request.action.startsWith('list-')"
]
```

---

## Performance Issues

### High Memory Usage

**Solutions**:
1. Reduce concurrent requests:
   ```toml
   [global]
   max_concurrent_requests = 4
   ```

2. For S3, adjust chunk sizes:
   ```toml
   [blob_store.s3]
   multipart_part_size = "10MiB"
   ```

### Slow Responses

**Solutions**:
1. Check storage latency
2. Enable Redis cache for multi-replica
3. Reduce webhook timeouts
4. Use immutable tags for cache optimization

---

## Getting Help

1. **Check logs**: Enable debug logging for the relevant module
2. **Verify config**: Test with minimal configuration
3. **Test isolation**: Isolate the failing component
4. **Report issues**: https://github.com/project-angos/angos/issues
