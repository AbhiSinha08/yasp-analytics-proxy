use super::*;

pub(super) fn source_permissions(
    client: &dyn Fn(&[&str]) -> Output,
    check: &dyn Fn(&[&str], &Output),
    secondary: &PostgresConnection,
    database_names: &[String; 2],
    route_one_marker: &str,
    callback_count: &AtomicUsize,
) {
    let owner = client(&["SELECT secret FROM public.yasp_private_probe /* route_owner */"]);
    check(&["owner-only"], &owner);
    let forbidden = secondary.database(&database_names[0]).query("SELECT 1");
    assert!(
        !forbidden.status.success(),
        "second source login unexpectedly connects to the first source database"
    );
    let allowed = secondary
        .database(&database_names[1])
        .query("SELECT current_user");
    assert!(
        allowed.status.success(),
        "second source login lacks its target database access"
    );
    let before = callback_count.load(Ordering::SeqCst);
    let private = client(&[
        "SELECT secret FROM public.yasp_private_probe /* route_two */",
        "SELECT current_database() || ':' || current_user",
    ]);
    let stderr = String::from_utf8_lossy(&private.stderr);
    assert!(
        stderr.contains("42501"),
        "the second source login unexpectedly read an owner-only table"
    );
    assert!(
        String::from_utf8_lossy(&private.stdout).contains(route_one_marker),
        "frontend session did not recover after a source permission error"
    );
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + 2,
        "selector runs once for each parsed read query"
    );

    let before = callback_count.load(Ordering::SeqCst);
    let invalid = client(&[
        "  ",
        "DELETE FROM public.users",
        "SELECT current_database() || ':' || current_user",
    ]);
    assert!(
        String::from_utf8_lossy(&invalid.stderr).contains("0A000"),
        "writes are rejected before routing"
    );
    assert!(
        String::from_utf8_lossy(&invalid.stdout).contains(route_one_marker),
        "session recovers after input validation"
    );
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        before + 1,
        "empty and rejected queries never reach the selector"
    );
}
