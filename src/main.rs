use steel_sled::build_module;

fn main() {
    build_module()
        .emit_package_to_file("libsteel_sled", "sled.scm")
        .unwrap()
}
