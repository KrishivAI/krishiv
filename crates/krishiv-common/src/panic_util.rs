/// Extract a human-readable message from a `catch_unwind` panic payload.
///
/// Handles the two common payload types (`&'static str` and `String`) and
/// falls back to a generic message for any other type.
pub fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        (*msg).to_owned()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_static_str() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("oops");
        assert_eq!(panic_payload_to_string(&*payload), "oops");
    }

    #[test]
    fn extracts_owned_string() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("owned"));
        assert_eq!(panic_payload_to_string(&*payload), "owned");
    }

    #[test]
    fn falls_back_for_unknown_type() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(
            panic_payload_to_string(&*payload),
            "non-string panic payload"
        );
    }
}
