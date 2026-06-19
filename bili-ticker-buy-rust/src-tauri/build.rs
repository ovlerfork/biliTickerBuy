fn main() {
    #[cfg(not(feature = "desktop"))]
    return;

    #[cfg(feature = "desktop")]
    tauri_build::build()
}
