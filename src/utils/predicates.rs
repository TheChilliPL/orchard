pub fn is_default<T: Default + PartialEq>(val: &T) -> bool {
    val == &Default::default()
}

pub fn is_false(val: &bool) -> bool {
    !*val
}

pub fn is_true(val: &bool) -> bool {
    *val
}
