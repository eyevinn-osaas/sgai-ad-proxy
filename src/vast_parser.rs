mod utils;
use utils::*;

fn main() {
    let vast = vast4_rs::from_str(include_str!("../test_data/vast4.1.xml")).unwrap();
    let linears = get_all_linears_from_vast(&vast);
    for linear in linears {
        let (duration, media_urls) = get_duration_and_media_urls_from_linear(&linear);
        println!("Duration: {}", duration);
        println!("Media URLs: {:?}", media_urls);
    }
}
