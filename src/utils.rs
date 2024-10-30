use actix_web::{dev::PeerAddr, web, HttpRequest, HttpResponseBuilder};
use awc::Client;
use url::Url;

pub fn get_all_linears_from_vast<'a>(vast: &'a vast4_rs::Vast<'a>) -> Vec<vast4_rs::Linear<'a>> {
    let ads = &vast.ads;
    ads.iter()
        .flat_map(|ad| {
            ad.in_line.iter().flat_map(|in_line| {
                in_line
                    .creatives
                    .creatives
                    .iter()
                    .filter_map(|createve| createve.linear.clone())
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>()
}

pub fn get_duration_from_linear(linear: &vast4_rs::Linear) -> u64 {
    linear
        .duration
        .as_ref()
        .map(|duration| std::time::Duration::from(duration.clone()).as_secs())
        .unwrap_or_default()
}

pub fn get_media_urls_from_linear(linear: &vast4_rs::Linear) -> Vec<String> {
    linear
        .media_files
        .as_ref()
        .map(|media_files| {
            media_files
                .media_files
                .iter()
                .map(|media_file| media_file.uri.clone().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

pub fn get_duration_and_media_urls_from_linear(linear: &vast4_rs::Linear) -> (u64, Vec<String>) {
    (
        get_duration_from_linear(linear),
        get_media_urls_from_linear(linear),
    )
}

pub fn fixed_offset_to_local(
    date: chrono::DateTime<chrono::FixedOffset>,
) -> chrono::DateTime<chrono::Local> {
    date.with_timezone(&chrono::Local)
}

pub fn build_forwarded_request(
    req: &HttpRequest,
    peer_addr: Option<PeerAddr>,
    client: web::Data<Client>,
    new_url: Url,
) -> awc::ClientRequest {
    let forwarded_req = client
        .request_from(new_url.as_str(), req.head())
        .no_decompress();

    match peer_addr {
        Some(PeerAddr(addr)) => {
            forwarded_req.insert_header(("x-forwarded-for", addr.ip().to_string()))
        }
        None => forwarded_req,
    }
}

pub fn copy_headers<T>(res: &awc::ClientResponse<T>, client_resp: &mut HttpResponseBuilder) {
    for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection") {
        client_resp.insert_header((header_name.clone(), header_value.clone()));
    }
}

pub fn build_forward_url(req: &HttpRequest, forward_url: &Url) -> Url {
    let mut new_url = forward_url.clone();
    new_url.set_path(req.uri().path());
    new_url.set_query(req.uri().query());
    new_url
}
