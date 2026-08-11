use std::{
    env,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use serde::Deserialize;
use serde_json::Value;
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

use crate::{
    error::{Error, Result},
    model::VideoCodec,
};

const FFPROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_PROBE_JSON_BYTES: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const FFPROBE_ADDRESS_SPACE_LIMIT_BYTES: libc::rlim_t = 1024 * 1024 * 1024;
#[cfg(unix)]
const FFPROBE_CPU_LIMIT_SECONDS: libc::rlim_t = 25;
#[cfg(not(windows))]
const FFPROBE_BINARY_NAME: &str = "ffprobe";
#[cfg(windows)]
const FFPROBE_BINARY_NAME: &str = "ffprobe.exe";

#[derive(Debug, Clone, PartialEq)]
pub struct MediaProbe {
    pub codec: VideoCodec,
    /// Display width after applying sample aspect ratio and orientation metadata.
    pub width: u32,
    /// Display height after applying sample aspect ratio and orientation metadata.
    pub height: u32,
    pub duration_seconds: Option<f64>,
}

/// Inspect a local media file with a fixed, non-shell ffprobe invocation.
pub async fn probe_media(path: impl AsRef<Path>) -> Result<MediaProbe> {
    let path = path.as_ref();
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| Error::InvalidMedia("媒体文件无法读取".to_owned()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(Error::InvalidMedia("媒体文件为空或不是普通文件".to_owned()));
    }

    let ffprobe_path = resolve_ffprobe_from_environment()?;
    let mut command = Command::new(ffprobe_path);
    command
        .kill_on_drop(true)
        .env_clear()
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg(
            "stream=codec_type,codec_name,width,height,sample_aspect_ratio,duration:stream_tags=rotate:stream_side_data=rotation,displaymatrix:stream_disposition=attached_pic,default:format=duration",
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
    apply_ffprobe_resource_limits(&mut command);

    let mut child = match command.spawn() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::Config("未找到 ffprobe 可执行文件".to_owned()));
        }
        Err(_) => {
            return Err(Error::Config(
                "无法启动 ffprobe，请检查执行权限和进程资源限制".to_owned(),
            ));
        }
        Ok(child) => child,
    };
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::InvalidMedia("无法读取 ffprobe 输出".to_owned()))?;

    let probe_result = timeout(FFPROBE_TIMEOUT, async move {
        let mut json = Vec::new();
        stdout
            .take((MAX_PROBE_JSON_BYTES + 1) as u64)
            .read_to_end(&mut json)
            .await
            .map_err(|_| Error::InvalidMedia("无法读取 ffprobe 输出".to_owned()))?;
        if json.len() > MAX_PROBE_JSON_BYTES {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(Error::InvalidMedia("ffprobe 返回的数据异常过大".to_owned()));
        }
        let status = child
            .wait()
            .await
            .map_err(|_| Error::InvalidMedia("无法等待 ffprobe".to_owned()))?;
        Ok((status, json))
    })
    .await;

    let (status, json) = match probe_result {
        Err(_) => return Err(Error::InvalidMedia("ffprobe 执行超时".to_owned())),
        Ok(result) => result?,
    };

    if !status.success() {
        let code = status
            .code()
            .map_or_else(|| "signal".to_owned(), |code| code.to_string());
        return Err(Error::InvalidMedia(format!(
            "ffprobe 检查失败（退出状态 {code}）"
        )));
    }
    parse_probe_json(&json)
}

#[cfg(unix)]
fn apply_ffprobe_resource_limits(command: &mut Command) {
    // SAFETY: the hook performs only async-signal-safe setrlimit syscalls and
    // constructs an errno-backed I/O error on failure. It runs in the child
    // immediately before exec, so it cannot affect the Bot process itself.
    unsafe {
        command.pre_exec(|| {
            #[cfg(target_os = "linux")]
            {
                let mut inherited = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::getrlimit(libc::RLIMIT_AS, &mut inherited) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let address_space = libc::rlimit {
                    rlim_cur: inherited.rlim_cur.min(FFPROBE_ADDRESS_SPACE_LIMIT_BYTES),
                    rlim_max: inherited.rlim_max.min(FFPROBE_ADDRESS_SPACE_LIMIT_BYTES),
                };
                if libc::setrlimit(libc::RLIMIT_AS, &address_space) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            let mut inherited = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::getrlimit(libc::RLIMIT_CPU, &mut inherited) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let cpu = libc::rlimit {
                rlim_cur: inherited.rlim_cur.min(FFPROBE_CPU_LIMIT_SECONDS),
                rlim_max: inherited.rlim_max.min(FFPROBE_CPU_LIMIT_SECONDS),
            };
            if libc::setrlimit(libc::RLIMIT_CPU, &cpu) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn apply_ffprobe_resource_limits(_command: &mut Command) {}

fn resolve_ffprobe_from_environment() -> Result<PathBuf> {
    let configured = env::var_os("FFPROBE_PATH");
    let search_path = env::var_os("PATH");
    resolve_ffprobe_path(configured.as_deref(), search_path.as_deref())
}

fn resolve_ffprobe_path(
    configured: Option<&OsStr>,
    search_path: Option<&OsStr>,
) -> Result<PathBuf> {
    if let Some(configured) = configured {
        let configured = Path::new(configured);
        if !configured.is_absolute() {
            return Err(invalid_configured_ffprobe());
        }
        return canonical_executable(configured).ok_or_else(invalid_configured_ffprobe);
    }

    ffprobe_candidates(search_path)
        .into_iter()
        .find_map(|candidate| canonical_executable(&candidate))
        .ok_or_else(|| {
            Error::Config("未找到安全的 ffprobe 可执行文件，请设置绝对路径 FFPROBE_PATH".to_owned())
        })
}

fn ffprobe_candidates(search_path: Option<&OsStr>) -> Vec<PathBuf> {
    search_path
        .map(env::split_paths)
        .into_iter()
        .flatten()
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join(FFPROBE_BINARY_NAME))
        .collect()
}

fn canonical_executable(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let canonical = fs::canonicalize(path).ok()?;
    if !canonical.is_absolute() {
        return None;
    }
    let metadata = fs::metadata(&canonical).ok()?;
    (is_executable_file(&metadata) && executable_path_is_trusted(&canonical, &metadata))
        .then_some(canonical)
}

#[cfg(unix)]
fn is_executable_file(metadata: &fs::Metadata) -> bool {
    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
}

#[cfg(unix)]
fn executable_path_is_trusted(path: &Path, metadata: &fs::Metadata) -> bool {
    // SAFETY: geteuid takes no pointers and has no preconditions.
    let effective_user_id = unsafe { libc::geteuid() };
    let trusted_owner = |owner| owner == 0 || owner == effective_user_id;
    if !trusted_owner(metadata.uid()) || metadata.permissions().mode() & 0o022 != 0 {
        return false;
    }
    path.ancestors().skip(1).all(|directory| {
        let Ok(metadata) = fs::metadata(directory) else {
            return false;
        };
        if !metadata.is_dir() || !trusted_owner(metadata.uid()) {
            return false;
        }
        let mode = metadata.permissions().mode();
        mode & 0o022 == 0 || mode & 0o1000 != 0
    })
}

#[cfg(not(unix))]
fn is_executable_file(metadata: &fs::Metadata) -> bool {
    metadata.is_file()
}

#[cfg(not(unix))]
fn executable_path_is_trusted(_path: &Path, _metadata: &fs::Metadata) -> bool {
    true
}

fn invalid_configured_ffprobe() -> Error {
    Error::Config("FFPROBE_PATH 必须是指向普通可执行文件的绝对路径".to_owned())
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
    sample_aspect_ratio: Option<Value>,
    #[serde(default)]
    duration: Option<Value>,
    #[serde(default)]
    disposition: ProbeDisposition,
    #[serde(default)]
    tags: ProbeTags,
    #[serde(default)]
    side_data_list: Vec<ProbeSideData>,
}

#[derive(Debug, Default, Deserialize)]
struct ProbeDisposition {
    #[serde(default)]
    attached_pic: i32,
    #[serde(default)]
    default: i32,
}

#[derive(Debug, Default, Deserialize)]
struct ProbeTags {
    #[serde(default)]
    rotate: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
struct ProbeSideData {
    #[serde(default)]
    rotation: Option<Value>,
    #[serde(default)]
    displaymatrix: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ProbeFormat {
    #[serde(default)]
    duration: Option<Value>,
}

fn parse_probe_json(json: &[u8]) -> Result<MediaProbe> {
    let document: ProbeDocument = serde_json::from_slice(json)
        .map_err(|_| Error::InvalidMedia("ffprobe 返回了无效 JSON".to_owned()))?;

    let video = choose_video_stream(&document.streams)
        .ok_or_else(|| Error::InvalidMedia("文件中没有视频流".to_owned()))?;
    let coded_width = video
        .width
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::InvalidMedia("视频宽度无效".to_owned()))?;
    let coded_height = video
        .height
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::InvalidMedia("视频高度无效".to_owned()))?;
    let (width, height) = visible_dimensions(video, coded_width, coded_height);

    let duration_seconds = parse_duration(video.duration.as_ref()).or_else(|| {
        document
            .format
            .as_ref()
            .and_then(|format| parse_duration(format.duration.as_ref()))
    });

    Ok(MediaProbe {
        codec: parse_video_codec(video.codec_name.as_deref()),
        width,
        height,
        duration_seconds,
    })
}

fn visible_dimensions(stream: &ProbeStream, coded_width: u32, coded_height: u32) -> (u32, u32) {
    // Keep the coded dimensions when optional metadata is malformed or its
    // result cannot be represented by Telegram's metadata types.
    let display_width = stream
        .sample_aspect_ratio
        .as_ref()
        .and_then(parse_sample_aspect_ratio)
        .and_then(|(numerator, denominator)| scale_dimension(coded_width, numerator, denominator))
        .unwrap_or(coded_width);

    if stream_has_quarter_turn(stream) {
        (coded_height, display_width)
    } else {
        (display_width, coded_height)
    }
}

fn parse_sample_aspect_ratio(value: &Value) -> Option<(u64, u64)> {
    let value = value.as_str()?.trim();
    let (numerator, denominator) = value.split_once(':')?;
    let numerator = numerator.trim().parse::<u64>().ok()?;
    let denominator = denominator.trim().parse::<u64>().ok()?;
    (numerator > 0 && denominator > 0).then_some((numerator, denominator))
}

fn scale_dimension(value: u32, numerator: u64, denominator: u64) -> Option<u32> {
    let scaled_numerator = u128::from(value).checked_mul(u128::from(numerator))?;
    let rounded =
        scaled_numerator.checked_add(u128::from(denominator) / 2)? / u128::from(denominator);
    u32::try_from(rounded).ok().filter(|value| *value > 0)
}

fn stream_has_quarter_turn(stream: &ProbeStream) -> bool {
    for side_data in &stream.side_data_list {
        if let Some(rotation) = side_data.rotation.as_ref().and_then(parse_rotation) {
            return rotation;
        }
        if let Some(rotation) = side_data
            .displaymatrix
            .as_ref()
            .and_then(parse_display_matrix_rotation)
        {
            return rotation;
        }
    }

    stream
        .tags
        .rotate
        .as_ref()
        .and_then(parse_rotation)
        .unwrap_or(false)
}

fn parse_rotation(value: &Value) -> Option<bool> {
    let degrees = match value {
        Value::String(value) => value.trim().parse::<f64>().ok()?,
        Value::Number(value) => value.as_f64()?,
        _ => return None,
    };
    classify_rotation(degrees)
}

fn classify_rotation(degrees: f64) -> Option<bool> {
    const EPSILON_DEGREES: f64 = 0.01;

    if !degrees.is_finite() {
        return None;
    }
    let normalized = degrees.rem_euclid(360.0);
    let close_to = |target: f64| {
        let distance = (normalized - target).abs();
        distance.min(360.0 - distance) <= EPSILON_DEGREES
    };

    if close_to(90.0) || close_to(270.0) {
        Some(true)
    } else if close_to(0.0) || close_to(180.0) {
        Some(false)
    } else {
        None
    }
}

fn parse_display_matrix_rotation(value: &Value) -> Option<bool> {
    let matrix = value.as_str()?;
    let mut rows = matrix
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let first = parse_display_matrix_row(rows.next()?)?;
    let second = parse_display_matrix_row(rows.next()?)?;

    let [a, b, _] = first.map(|value| value as f64);
    let [c, d, _] = second.map(|value| value as f64);
    let first_norm = a.hypot(c);
    let second_norm = b.hypot(d);
    if first_norm == 0.0 || second_norm == 0.0 {
        return None;
    }

    // A display matrix may also scale or mirror a video, but its two axes must
    // remain approximately orthogonal before it is safe to treat it as a pure
    // orientation hint.
    let dot = a.mul_add(b, c * d).abs();
    if dot > first_norm * second_norm * 0.01 {
        return None;
    }

    classify_rotation(c.atan2(a).to_degrees())
}

fn parse_display_matrix_row(line: &str) -> Option<[i64; 3]> {
    let (_, values) = line.split_once(':')?;
    let mut values = values.split_whitespace();
    let first = values.next()?.parse::<i64>().ok()?;
    let second = values.next()?.parse::<i64>().ok()?;
    let third = values.next()?.parse::<i64>().ok()?;
    Some([first, second, third])
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
    fn configured_ffprobe_path_must_be_absolute() {
        assert!(matches!(
            resolve_ffprobe_path(Some(OsStr::new("bin/ffprobe")), None),
            Err(Error::Config(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn path_candidates_skip_relative_directories() {
        let search_path = env::join_paths([
            PathBuf::from("relative/bin"),
            PathBuf::from("."),
            PathBuf::new(),
            PathBuf::from("/trusted/media/bin"),
        ])
        .unwrap();

        assert_eq!(
            ffprobe_candidates(Some(&search_path)),
            vec![PathBuf::from("/trusted/media/bin").join(FFPROBE_BINARY_NAME)]
        );
    }

    #[cfg(unix)]
    #[test]
    fn configured_ffprobe_is_canonicalized() {
        let directory = TestDirectory::new();
        let executable = directory.create_file("real-ffprobe", 0o700);
        let configured = directory.path().join(FFPROBE_BINARY_NAME);
        std::os::unix::fs::symlink(&executable, &configured).unwrap();

        let resolved =
            resolve_ffprobe_path(Some(configured.as_os_str()), None).expect("valid executable");

        assert!(resolved.is_absolute());
        assert_eq!(resolved, fs::canonicalize(executable).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn configured_ffprobe_must_be_a_regular_executable_file() {
        let directory = TestDirectory::new();
        let non_executable = directory.create_file("not-executable", 0o600);
        let writable_by_others = directory.create_file("writable-by-others", 0o722);

        assert!(matches!(
            resolve_ffprobe_path(Some(non_executable.as_os_str()), None),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            resolve_ffprobe_path(Some(directory.path().as_os_str()), None),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            resolve_ffprobe_path(Some(writable_by_others.as_os_str()), None),
            Err(Error::Config(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn path_search_skips_invalid_files_and_returns_a_canonical_executable() {
        let invalid_directory = TestDirectory::new();
        invalid_directory.create_file(FFPROBE_BINARY_NAME, 0o600);
        let valid_directory = TestDirectory::new();
        let executable = valid_directory.create_file(FFPROBE_BINARY_NAME, 0o700);
        let search_path = env::join_paths([invalid_directory.path(), valid_directory.path()])
            .expect("valid test PATH");

        let resolved =
            resolve_ffprobe_path(None, Some(&search_path)).expect("PATH executable found");

        assert_eq!(resolved, fs::canonicalize(executable).unwrap());
    }

    #[cfg(unix)]
    struct TestDirectory(PathBuf);

    #[cfg(unix)]
    impl TestDirectory {
        fn new() -> Self {
            let path =
                env::temp_dir().join(format!("parse-bot-ffprobe-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn create_file(&self, name: &str, mode: u32) -> PathBuf {
            let path = self.0.join(name);
            fs::write(&path, b"test executable").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            path
        }
    }

    #[cfg(unix)]
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

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
            Err(Error::InvalidMedia(_))
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

    #[test]
    fn applies_tag_rotation_of_90_degrees() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "tags": {"rotate": "90"}
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (1080, 1920));
    }

    #[test]
    fn applies_side_data_rotation_of_270_degrees() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "hevc",
                "width": 1080,
                "height": 1920,
                "side_data_list": [{"rotation": 270}]
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (1920, 1080));
    }

    #[test]
    fn derives_quarter_turn_from_display_matrix() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "side_data_list": [{
                    "displaymatrix": "\n00000000:            0      -65536           0\n00000001:        65536           0           0\n00000002:            0           0  1073741824\n"
                }]
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (1080, 1920));
    }

    #[test]
    fn applies_sample_aspect_ratio_before_rotation() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 720,
                "height": 576,
                "sample_aspect_ratio": "16:15",
                "side_data_list": [{"rotation": -90}]
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (576, 768));
    }

    #[test]
    fn ignores_invalid_orientation_and_aspect_ratio_metadata() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 640,
                "height": 360,
                "sample_aspect_ratio": "0:0",
                "tags": {"rotate": "not-a-number"},
                "side_data_list": [{
                    "rotation": "NaN",
                    "displaymatrix": "not a display matrix"
                }]
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (640, 360));
    }

    #[test]
    fn ignores_sample_aspect_ratio_that_overflows_safe_dimensions() {
        let json = br#"{
            "streams": [{
                "codec_type": "video",
                "codec_name": "h264",
                "width": 4294967295,
                "height": 2160,
                "sample_aspect_ratio": "18446744073709551615:1"
            }]
        }"#;

        let probe = parse_probe_json(json).unwrap();
        assert_eq!((probe.width, probe.height), (u32::MAX, 2160));
    }
}
