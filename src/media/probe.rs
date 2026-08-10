use std::{path::Path, process::Stdio, time::Duration};

use serde::Deserialize;
use serde_json::Value;
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

use crate::{
    error::{AppError, Result},
    model::VideoCodec,
};

const FFPROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_PROBE_JSON_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct MediaProbe {
    pub codec: VideoCodec,
    pub has_audio: bool,
    pub width: u32,
    pub height: u32,
    pub duration_seconds: Option<f64>,
}

/// Inspect a local media file with a fixed, non-shell ffprobe invocation.
pub async fn probe_media(path: impl AsRef<Path>) -> Result<MediaProbe> {
    let path = path.as_ref();
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| AppError::InvalidMedia("媒体文件无法读取".to_owned()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(AppError::InvalidMedia(
            "媒体文件为空或不是普通文件".to_owned(),
        ));
    }

    let mut command = Command::new("ffprobe");
    command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg(
            "stream=codec_type,codec_name,width,height,duration:stream_disposition=attached_pic,default:format=duration",
        )
        .arg("-of")
        .arg("json")
        // The downloaded file is untrusted. Without these restrictions ffprobe
        // can recognize HLS/DASH playlists and perform a second network request,
        // bypassing the downloader's CDN and DNS checks.
        .arg("-protocol_whitelist")
        .arg("file")
        .arg("-format_whitelist")
        .arg("mov")
        .arg("-i")
        .arg(path);

    let mut child = match command.spawn() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::Config("未找到 ffprobe 可执行文件".to_owned()));
        }
        Err(_) => return Err(AppError::InvalidMedia("无法启动 ffprobe".to_owned())),
        Ok(child) => child,
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::InvalidMedia("无法读取 ffprobe 输出".to_owned()))?;

    let probe_result = timeout(FFPROBE_TIMEOUT, async move {
        let mut json = Vec::new();
        stdout
            .take((MAX_PROBE_JSON_BYTES + 1) as u64)
            .read_to_end(&mut json)
            .await
            .map_err(|_| AppError::InvalidMedia("无法读取 ffprobe 输出".to_owned()))?;
        if json.len() > MAX_PROBE_JSON_BYTES {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(AppError::InvalidMedia(
                "ffprobe 返回的数据异常过大".to_owned(),
            ));
        }
        let status = child
            .wait()
            .await
            .map_err(|_| AppError::InvalidMedia("无法等待 ffprobe".to_owned()))?;
        Ok((status, json))
    })
    .await;

    let (status, json) = match probe_result {
        Err(_) => return Err(AppError::InvalidMedia("ffprobe 执行超时".to_owned())),
        Ok(result) => result?,
    };

    if !status.success() {
        let code = status
            .code()
            .map_or_else(|| "signal".to_owned(), |code| code.to_string());
        return Err(AppError::InvalidMedia(format!(
            "ffprobe 检查失败（退出状态 {code}）"
        )));
    }
    parse_probe_json(&json)
}

#[derive(Debug, Deserialize)]
struct ProbeDocument {
    #[serde(default)]
    streams: Vec<ProbeStream>,
    #[serde(default)]
    format: Option<ProbeFormat>,
}

#[derive(Debug, Deserialize)]
struct ProbeStream {
    #[serde(default)]
    codec_type: Option<String>,
    #[serde(default)]
    codec_name: Option<String>,
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    duration: Option<Value>,
    #[serde(default)]
    disposition: ProbeDisposition,
}

#[derive(Debug, Default, Deserialize)]
struct ProbeDisposition {
    #[serde(default)]
    attached_pic: i32,
    #[serde(default)]
    default: i32,
}

#[derive(Debug, Deserialize)]
struct ProbeFormat {
    #[serde(default)]
    duration: Option<Value>,
}

fn parse_probe_json(json: &[u8]) -> Result<MediaProbe> {
    let document: ProbeDocument = serde_json::from_slice(json)
        .map_err(|_| AppError::InvalidMedia("ffprobe 返回了无效 JSON".to_owned()))?;

    let has_audio = document
        .streams
        .iter()
        .any(|stream| stream.codec_type.as_deref() == Some("audio"));

    let video = choose_video_stream(&document.streams)
        .ok_or_else(|| AppError::InvalidMedia("文件中没有视频流".to_owned()))?;
    let width = video
        .width
        .filter(|value| *value > 0)
        .ok_or_else(|| AppError::InvalidMedia("视频宽度无效".to_owned()))?;
    let height = video
        .height
        .filter(|value| *value > 0)
        .ok_or_else(|| AppError::InvalidMedia("视频高度无效".to_owned()))?;

    let duration_seconds = parse_duration(video.duration.as_ref()).or_else(|| {
        document
            .format
            .as_ref()
            .and_then(|format| parse_duration(format.duration.as_ref()))
    });

    Ok(MediaProbe {
        codec: parse_video_codec(video.codec_name.as_deref()),
        has_audio,
        width,
        height,
        duration_seconds,
    })
}

fn choose_video_stream(streams: &[ProbeStream]) -> Option<&ProbeStream> {
    streams
        .iter()
        .filter(|stream| {
            stream.codec_type.as_deref() == Some("video")
                && stream.disposition.attached_pic == 0
                && stream.width.is_some_and(|width| width > 0)
                && stream.height.is_some_and(|height| height > 0)
        })
        .max_by_key(|stream| {
            let is_default = u64::from(stream.disposition.default > 0);
            let area = u64::from(stream.width.unwrap_or(0))
                .saturating_mul(u64::from(stream.height.unwrap_or(0)));
            (is_default, area)
        })
}

fn parse_video_codec(codec_name: Option<&str>) -> VideoCodec {
    match codec_name.map(str::to_ascii_lowercase).as_deref() {
        Some("h264" | "avc" | "avc1") => VideoCodec::H264,
        Some("hevc" | "h265" | "hev1" | "hvc1") => VideoCodec::H265,
        _ => VideoCodec::Unknown,
    }
}

fn parse_duration(value: Option<&Value>) -> Option<f64> {
    let duration = match value? {
        Value::String(value) => value.parse::<f64>().ok()?,
        Value::Number(value) => value.as_f64()?,
        _ => return None,
    };

    (duration.is_finite() && duration >= 0.0).then_some(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_video_audio_dimensions_codec_and_duration() {
        let json = br#"{
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "hevc",
                    "width": 1920,
                    "height": 1080,
                    "duration": "12.375",
                    "disposition": {"default": 1, "attached_pic": 0}
                },
                {"codec_type": "audio", "codec_name": "aac"}
            ],
            "format": {"duration": "12.400"}
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!(probe.codec, VideoCodec::H265);
        assert!(probe.has_audio);
        assert_eq!((probe.width, probe.height), (1920, 1080));
        assert_eq!(probe.duration_seconds, Some(12.375));
    }

    #[test]
    fn ignores_cover_art_and_uses_format_duration() {
        let json = br#"{
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "mjpeg",
                    "width": 3000,
                    "height": 3000,
                    "disposition": {"attached_pic": 1}
                },
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 720,
                    "height": 1280,
                    "disposition": {"attached_pic": 0}
                }
            ],
            "format": {"duration": "3.5"}
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!(probe.codec, VideoCodec::H264);
        assert!(!probe.has_audio);
        assert_eq!((probe.width, probe.height), (720, 1280));
        assert_eq!(probe.duration_seconds, Some(3.5));
    }

    #[test]
    fn rejects_documents_without_a_real_video_stream() {
        let json = br#"{
            "streams": [
                {"codec_type": "audio", "codec_name": "aac"},
                {
                    "codec_type": "video",
                    "codec_name": "mjpeg",
                    "width": 600,
                    "height": 600,
                    "disposition": {"attached_pic": 1}
                }
            ],
            "format": {"duration": "9.0"}
        }"#;

        assert!(matches!(
            parse_probe_json(json),
            Err(AppError::InvalidMedia(_))
        ));
    }

    #[test]
    fn handles_unknown_codec_and_non_finite_duration() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "vp9",
                "width": 640,
                "height": 360,
                "duration": "NaN"
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!(probe.codec, VideoCodec::Unknown);
        assert_eq!(probe.duration_seconds, None);
    }

    #[test]
    fn skips_default_video_stream_with_invalid_dimensions() {
        let json = br#"{
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 0,
                    "height": 0,
                    "disposition": {"default": 1}
                },
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1280,
                    "height": 720,
                    "disposition": {"default": 0}
                }
            ]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (1280, 720));
    }
}
