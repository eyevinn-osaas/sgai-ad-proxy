use actix_web::{dev::PeerAddr, web, HttpRequest, HttpResponseBuilder};
use awc::Client;
use rustls::{ClientConfig, RootCertStore};
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

/// Create simple rustls client config from root certificates.
pub fn rustls_config() -> ClientConfig {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    let root_store = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.to_owned());

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
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

pub fn build_advanced_ad_server_url(
    server_url: &Url,
    duration: u64,
    _pod_num: u64,
    user_id: &str,
) -> Url {
    let mut ad_url = server_url.clone();
    ad_url
        .query_pairs_mut()
        .clear()
        .append_pair("duration", &duration.to_string())
        .append_pair("channelId", "20007")
        .append_pair("userId", user_id)
        .append_pair("deviceType", "stb")
        .append_pair("os", "ios")
        .append_pair("screenSize", "960x540")
        .append_pair("geolocation", "176.71.21.0")
        .append_pair("useg", "a50-54,p4714,gm")
        .append_pair("adLimit", "0");

    ad_url
}

pub fn build_test_ad_server_url(
    server_url: &Url,
    duration: u64,
    pod_num: u64,
    user_id: &str,
) -> Url {
    let mut ad_url = server_url.clone();
    ad_url
        .query_pairs_mut()
        .append_pair("uid", user_id)
        .append_pair("dur", &duration.to_string())
        .append_pair("max", "5")
        .append_pair("min", "5")
        .append_pair("skip", "2")
        .append_pair("pod", &pod_num.to_string());

    ad_url
}
