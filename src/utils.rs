#[macro_export]
macro_rules! log_only_if {
    // macro input
    ($a:expr, $b:expr, $c:expr, $d:expr) => {
        // macro expand to this code
        {
            if $a {
                debug!($b, $c, $d)
            }
        }
    };
}
