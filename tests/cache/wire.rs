use super::*;

#[test]
fn malformed_cached_responses_are_misses() {
    let fixture = CacheScenario::new();
    let namespace = fixture.namespace.name.clone();
    let redis_url = fixture.redis_url.clone();
    let user = fixture.identity.user.clone();
    let password = fixture.identity.password.clone();
    let database = fixture.identity.database.clone();
    let make_backends = || fixture.backends();
    let cache =
        Arc::new(RedisCache::new(settings(&namespace, 60, 5, 196608), &redis_url, None).unwrap());
    let server = start_server(
        fixture.identity.gateway_config().with_cache(cache),
        make_backends(),
        fixture.selector(),
    );
    let address = server.address;
    let client = |query: &str| psql(query, &address, &user, &password, &database);
    const HEADER_BYTES: usize = 5 + 32;
    let wire_query = "SELECT 8675309";
    let wire_key = cache_key(&namespace, &fixture.route, wire_query);
    let wire_fill = client(wire_query);
    assert_success(&wire_fill, "cache wire fixture query");
    assert_eq!(stdout(&wire_fill).trim(), "8675309");
    let mut entry = redis_get(&redis_url, &wire_key);
    assert!(entry.len() > HEADER_BYTES + 7);
    assert_eq!(&entry[..5], b"YASP\x01");
    let mut offset = HEADER_BYTES;
    let mut row_description = false;
    while offset + 5 <= entry.len() {
        let packet_len =
            u32::from_be_bytes(entry[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let packet_end = offset + 1 + packet_len;
        assert!(packet_len >= 4 && packet_end <= entry.len());
        if entry[offset] == b'T' {
            assert_eq!(&entry[offset + 5..offset + 7], &[0, 1]);
            entry[offset + 5..offset + 7].copy_from_slice(&2u16.to_be_bytes());
            row_description = true;
            break;
        }
        offset = packet_end;
    }
    assert!(row_description, "cached response contains RowDescription");
    redis_set_bytes(&redis_url, &wire_key, &entry);
    let wire_miss = client(wire_query);
    assert_success(&wire_miss, "malformed cached response is a miss");
    assert_eq!(stdout(&wire_miss).trim(), "8675309");

    let entry = redis_get(&redis_url, &wire_key);
    let mut offset = HEADER_BYTES;
    while offset + 5 <= entry.len() {
        let packet_len =
            u32::from_be_bytes(entry[offset + 1..offset + 5].try_into().unwrap()) as usize;
        let packet_end = offset + 1 + packet_len;
        assert!(packet_len >= 4 && packet_end <= entry.len());
        if entry[offset] == b'C' {
            assert_eq!(packet_end, entry.len());
            break;
        }
        offset = packet_end;
    }
    assert_eq!(entry.get(offset), Some(&b'C'));
    let tag = format!("SELECT {}1", "0".repeat(30000));
    let mut long_tag_entry = entry[..offset].to_vec();
    long_tag_entry.push(b'C');
    long_tag_entry.extend_from_slice(&((4 + tag.len() + 1) as u32).to_be_bytes());
    long_tag_entry.extend_from_slice(tag.as_bytes());
    long_tag_entry.push(0);
    redis_set_bytes(&redis_url, &wire_key, &long_tag_entry);
    let long_tag_miss = client(wire_query);
    assert_success(&long_tag_miss, "oversized completion tag is a cache miss");
    assert_eq!(stdout(&long_tag_miss).trim(), "8675309");
}
