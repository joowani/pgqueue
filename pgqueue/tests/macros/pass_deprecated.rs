#![deny(warnings)]

#[pgqueue::job]
#[deprecated(note = "use replacement instead")]
pub async fn legacy(_: ()) {}

fn main() {
    #[allow(deprecated)]
    let _ = legacy::job(());
}
