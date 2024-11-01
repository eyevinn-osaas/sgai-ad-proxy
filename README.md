# Server-Guided Ad Insertion Proxy

This application is a simple http proxy server that inserts ads into a video stream. It is designed to be used in conjunction with a video player (e.g., AVPlayer) that supports Server Guided Ad Insertion (SGAI). The proxy server intercepts the video stream from the origin server and inserts ads into the media playlist as interstitals at specifed timepoints.

## Getting Started

### Prerequisites

* An HLS streaming sever

```bash
# Use ffmpeg to create a simple HLS Live stream under the "test" directory
ffmpeg -y -re -i sintel_trailer-1080p.mp4 \
  -preset slow -g 48 -sc_threshold 0 \
  -map 0:0 -map 0:1 -map 0:0 -map 0:1 \
  -s:v:0 640x360 -c:v:0 libx264 -b:v:0 365k \
  -s:v:1 960x540 -c:v:1 libx264 -b:v:1 2000k  \
  -c:a copy \
  -var_stream_map "v:0,a:0 v:1,a:1" \
  -master_pl_name master.m3u8 \
  -f hls -hls_time 4 -hls_list_size 8 -hls_delete_threshold 10 -hls_flags round_durations -hls_flags program_date_time \
  -hls_segment_filename "test/v%v/fileSequence%d.ts" test/v%v/media.m3u8

# Serve the HLS Live stream using a simple http server **above** the "test" directory
python -m http.server 8001

# Now you can access the HLS stream at http://127.0.0.1:8001/test/master.m3u8
```

* A running instance of the [test-ad-server](https://github.com/Eyevinn/test-adserver)

```bash
# Start the test ad server on port 8000
export PORT=8000

npm install
npm start

# Now you can access the test ad server at http://127.0.0.1:8000/api/docs
```

* AvPlayer or any other video player that supports Server Guided Ad Insertion (SGAI)

```bash
# Once the ad-proxy server is running (e.g., on port 3333), 
# you can use AVPlayer to play the HLS stream
```

### Run

```bash
# Start the ad-proxy server on port 3333 with origin server on port 8001 and test ad server on port 8000
# The master playlist is located at test/master.m3u8 in the origin server
# Use dynamic mode to insert ads into the HLS Live stream at specified timepoints
cargo run --bin ad_proxy 127.0.0.1 3333 127.0.0.1 8001 test/master.m3u8 http://localhost:8000/api/v1/vast -i dynamic

# Now you can access the HLS Live stream at http://127.0.0.1:3333/test/master.m3u8
# NOTE: Each proxy server instance can only handle one HLS Live stream at a time and restart is required to switch streams
```

For more options, run `ad_proxy --help`

```bash
Usage: ad_proxy [OPTIONS] <LISTEN_ADDR> <LISTEN_PORT> <FORWARD_ADDR> <FORWARD_PORT> <AD_SERVER_ENDPOINT>

Arguments:
  <LISTEN_ADDR>           Proxy address (ip)
  <LISTEN_PORT>           Proxy port
  <FORWARD_ADDR>          Origin server address (ip)
  <FORWARD_PORT>          Origin server port
  <MASTER_PLAYLIST_PATH>  Path to the master playlist (test/master.m3u8)
  <AD_SERVER_ENDPOINT>    Ad server endpoint (protocol://ip:port/path)
                          It should be a VAST4.0/4.1 XML endpoint
Options:
  -a, --ad-server-mode <AD_SERVER_MODE>
    Ad server to use:
    1) default  - use default test ad server
    2) advanced - use custom ad server [default: default]
  -i, --insertion-mode <INSERTION_MODE>
    Ad insertion mode to use:
    1) static  - add intertistial every 30 seconds (10 in total).
    2) dynamic - add intertistial when requested (Live Content only). [default: static]
```

### Insert Ads

One can insert ads into the video stream by sending a GET request to the ad-proxy server with the following query parameters:

* in - the time in seconds when the ad break should be inserted
* duration - the duration of the ad break in seconds
* pod_num - the number of creatives in this ad break

For example, to insert an ad break at 5 seconds from the live-edge with a duration of 10 seconds and 2 creatives, one would send the following request:

```bash
curl http://127.0.0.1:3333/command?in=5&dur=10&pod=2
```

### Example Modified Media Playlist

```m3u8
#EXTM3U
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:11
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:27.853+0100
#EXTINF:4,
fileSequence11.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:31.853+0100
#EXTINF:4,
fileSequence12.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:35.853+0100
#EXTINF:4,
fileSequence13.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:39.853+0100
#EXTINF:4,
fileSequence14.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:43.853+0100
#EXTINF:4,
fileSequence15.ts
#EXT-X-DATERANGE:ID="ad_slot0",CLASS="com.apple.hls.interstitial",START-DATE="2024-10-30T12:52:47.207+01:00",DURATION=10,X-ASSET-LIST="http://127.0.0.1:3333/interstitials.m3u8?_HLS_interstitial_id=ad_slot0",X-RESTRICT="SKIP,JUMP",X-RESUME-OFFSET=10,X-SNAP="IN,OUT"
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:47.853+0100
#EXTINF:4,
fileSequence16.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:51.853+0100
#EXTINF:4,
fileSequence17.ts
#EXT-X-PROGRAM-DATE-TIME:2024-10-30T12:52:55.853+0100
#EXTINF:4,
fileSequence18.ts
```

## License (Apache-2.0)

Copyright 2023 Eyevinn Technology AB

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support

Join our [community on Slack](http://slack.streamingtech.se) where you can post any questions regarding any of our open source projects. Eyevinn's consulting business can also offer you:

- Further development of this component
- Customization and integration of this component into your platform
- Support and maintenance agreement

Contact [sales@eyevinn.se](mailto:sales@eyevinn.se) if you are interested.

## About Eyevinn Technology

Eyevinn Technology is an independent consultant firm specialized in video and streaming. Independent in a way that we are not commercially tied to any platform or technology vendor.

At Eyevinn, every software developer consultant has a dedicated budget reserved for open source development and contribution to the open source community. This give us room for innovation, team building and personal competence development. And also gives us as a company a way to contribute back to the open source community.

Want to know more about Eyevinn and how it is to work here. Contact us at <work@eyevinn.se>!
