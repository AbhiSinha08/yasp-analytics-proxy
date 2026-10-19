use super::result;
use arrow_array::{
    ArrayRef, BinaryArray, Decimal128Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use std::sync::Arc;

#[test]
fn values_preserve_precision_and_reject_loss_or_oversized_frames() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal128Array::from(vec![
                Some(-12_345_678_901_234_567_890_123_456_789_i128),
                Some(1),
            ])
            .with_precision_and_scale(38, 9)
            .unwrap(),
        ),
        Arc::new(BinaryArray::from(vec![Some(&b"\0\xff"[..]), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(-1_000), Some(1)])),
        Arc::new(StringArray::from(vec![Some("snow"), None])),
    ];
    let batch = RecordBatch::try_from_iter([
        ("N", arrays[0].clone()),
        ("B", arrays[1].clone()),
        ("T", arrays[2].clone()),
        ("S", arrays[3].clone()),
    ])
    .unwrap();
    assert_eq!(
        result::columns(batch.schema().as_ref(), 4096)
            .unwrap()
            .len(),
        4
    );
    let row = result::row(&batch, 0, 4096).unwrap();
    let text: Vec<_> = row
        .iter()
        .map(|v| v.as_ref().map(|v| std::str::from_utf8(v).unwrap()))
        .collect();
    assert_eq!(
        text,
        vec![
            Some("-12345678901234567890.123456789"),
            Some("\\x00ff"),
            Some("1969-12-31 23:59:59.999999"),
            Some("snow")
        ]
    );
    assert!(
        result::row(&batch, 1, 4096).is_err(),
        "nanoseconds must not be rounded to microseconds"
    );
    assert!(result::row(&batch, 0, 32).is_err());
    assert!(result::columns(batch.schema().as_ref(), 8).is_err());
    let null = RecordBatch::try_from_iter([(
        "Z",
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
    )])
    .unwrap();
    assert!(result::row(&null, 0, 32).unwrap()[0].is_none());
    for column in [
        Arc::new(
            Decimal128Array::from(vec![99])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ) as ArrayRef,
        Arc::new(StringArray::from(vec!["contains\0NUL"])) as ArrayRef,
        Arc::new(arrow_array::Date32Array::from(vec![-719_528])) as ArrayRef,
    ] {
        let invalid = RecordBatch::try_from_iter([("invalid", column)]).unwrap();
        assert!(
            result::row(&invalid, 0, 4096).is_err(),
            "invalid frontend values must not escape as successful rows"
        );
    }
}
