use krishiv_api::Session;
#[tokio::main]
async fn main() {
    let session = Session::from_env().unwrap();
    let result = session.sql("SELECT 1 as baremetal_test").unwrap().collect().unwrap();
    println!("{}", result.pretty().unwrap());
}
